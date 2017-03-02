/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.columniterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.utils.ByteBufferUtil;

abstract class AbstractSSTableIterator implements UnfilteredRowIterator
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractSSTableIterator.class);

    protected final SSTableReader sstable;
    // We could use sstable.metadata(), but that can change during execution so it's good hygiene to grab an immutable instance
    protected final TableMetadata metadata;

    protected final DecoratedKey key;
    protected final DeletionTime partitionLevelDeletion;
    protected final ColumnFilter columns;
    protected final SerializationHelper helper;

    protected final Row staticRow;
    protected final Reader reader;

    private boolean isClosed;

    protected final Slices slices;

    @SuppressWarnings("resource") // We need this because the analysis is not able to determine that we do close
                                  // file on every path where we created it.
    protected AbstractSSTableIterator(SSTableReader sstable,
                                      FileDataInput file,
                                      DecoratedKey key,
                                      IndexedEntry indexEntry,
                                      Slices slices,
                                      ColumnFilter columnFilter)
    {
        this.sstable = sstable;
        this.metadata = sstable.metadata();
        this.key = key;
        this.columns = columnFilter;
        this.slices = slices;
        this.helper = new SerializationHelper(metadata, sstable.descriptor.version.correspondingMessagingVersion(), SerializationHelper.Flag.LOCAL, columnFilter);

        if (indexEntry == null)
        {
            this.partitionLevelDeletion = DeletionTime.LIVE;
            this.reader = null;
            this.staticRow = Rows.EMPTY_STATIC_ROW;
        }
        else
        {
            boolean shouldCloseFile = file == null;
            try
            {
                // We seek to the beginning to the partition if either:
                //   - the partition is not indexed; we then have a single block to read anyway
                //     (and we need to read the partition deletion time).
                //   - we're querying static columns.
                boolean needSeekAtPartitionStart = !indexEntry.isIndexed() || !columns.fetchedColumns().statics.isEmpty();

                if (needSeekAtPartitionStart)
                {
                    // Not indexed (or is reading static), set to the beginning of the partition and read partition level deletion there
                    if (file == null)
                        file = sstable.getFileDataInput(indexEntry.getPosition());
                    else
                        file.seek(indexEntry.getPosition());

                    ByteBufferUtil.skipShortLength(file); // Skip partition key
                    this.partitionLevelDeletion = DeletionTime.serializer.deserialize(file);

                    // Note that this needs to be called after file != null and after the partitionDeletion has been set, but before readStaticRow
                    // (since it uses it) so we can't move that up (but we'll be able to simplify as soon as we drop support for the old file format).
                    this.reader = createReader(indexEntry, file, shouldCloseFile);
                    this.staticRow = readStaticRow(sstable, file, helper, columns.fetchedColumns().statics);
                }
                else
                {
                    this.partitionLevelDeletion = indexEntry.deletionTime();
                    this.staticRow = Rows.EMPTY_STATIC_ROW;
                    this.reader = createReader(indexEntry, file, shouldCloseFile);
                }

                if (reader != null && !slices.isEmpty())
                    reader.setForSlice(nextSlice());

                if (reader == null && file != null && shouldCloseFile)
                    file.close();
            }
            catch (IOException e)
            {
                sstable.markSuspect();
                String filePath = file.getPath();
                if (shouldCloseFile)
                {
                    try
                    {
                        file.close();
                    }
                    catch (IOException suppressed)
                    {
                        e.addSuppressed(suppressed);
                    }
                }
                throw new CorruptSSTableException(e, filePath);
            }
        }
    }

    private Slice nextSlice()
    {
        return slices.get(nextSliceIndex());
    }

    /**
     * Returns the index of the next slice to process.
     * @return the index of the next slice to process
     */
    protected abstract int nextSliceIndex();

    /**
     * Checks if there are more slice to process.
     * @return {@code true} if there are more slice to process, {@code false} otherwise.
     */
    protected abstract boolean hasMoreSlices();

    private static Row readStaticRow(SSTableReader sstable,
                                     FileDataInput file,
                                     SerializationHelper helper,
                                     Columns statics) throws IOException
    {
        if (!sstable.header.hasStatic())
            return Rows.EMPTY_STATIC_ROW;

        if (statics.isEmpty())
        {
            UnfilteredSerializer.serializer.skipStaticRow(file, sstable.header, helper);
            return Rows.EMPTY_STATIC_ROW;
        }
        else
        {
            return UnfilteredSerializer.serializer.deserializeStaticRow(file, sstable.header, helper);
        }
    }

    protected abstract Reader createReaderInternal(IndexedEntry indexEntry, FileDataInput file, boolean shouldCloseFile);

    private Reader createReader(IndexedEntry indexEntry, FileDataInput file, boolean shouldCloseFile)
    {
        return slices.isEmpty() ? new NoRowsReader(file, shouldCloseFile)
                                : createReaderInternal(indexEntry, file, shouldCloseFile);
    };

    public TableMetadata metadata()
    {
        return metadata;
    }

    public RegularAndStaticColumns columns()
    {
        return columns.fetchedColumns();
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public EncodingStats stats()
    {
        return sstable.stats();
    }

    public boolean hasNext()
    {
        while (true)
        {
            if (reader == null)
                return false;

            if (reader.hasNext())
                return true;

            if (!hasMoreSlices())
                return false;

            slice(nextSlice());
        }
    }

    public Unfiltered next()
    {
        assert reader != null;
        return reader.next();
    }

    private void slice(Slice slice)
    {
        try
        {
            if (reader != null)
                reader.setForSlice(slice);
        }
        catch (IOException e)
        {
            try
            {
                closeInternal();
            }
            catch (IOException suppressed)
            {
                e.addSuppressed(suppressed);
            }
            sstable.markSuspect();
            throw new CorruptSSTableException(e, reader.file.getPath());
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private void closeInternal() throws IOException
    {
        // It's important to make closing idempotent since it would bad to double-close 'file' as its a RandomAccessReader
        // and its close is not idemptotent in the case where we recycle it.
        if (isClosed)
            return;

        if (reader != null)
            reader.close();

        isClosed = true;
    }

    public void close()
    {
        try
        {
            closeInternal();
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, reader.file.getPath());
        }
    }

    protected abstract class Reader implements Iterator<Unfiltered>
    {
        private final boolean shouldCloseFile;
        public FileDataInput file;

        protected UnfilteredDeserializer deserializer;

        // Records the currently open range tombstone (if any)
        protected DeletionTime openMarker = null;

        protected Reader(FileDataInput file, boolean shouldCloseFile)
        {
            this.file = file;
            this.shouldCloseFile = shouldCloseFile;

            if (file != null)
                createDeserializer();
        }

        private void createDeserializer()
        {
            assert file != null && deserializer == null;
            deserializer = UnfilteredDeserializer.create(metadata, file, sstable.header, helper);
        }

        protected void seekToPosition(long position) throws IOException
        {
            logger.info("seekToPosition called for position {}", position);
            // This may be the first time we're actually looking into the file
            if (file == null)
            {
                file = sstable.getFileDataInput(position);
                createDeserializer();
            }
            else
            {
                file.seek(position);
            }
        }

        protected void updateOpenMarker(RangeTombstoneMarker marker)
        {
            // Note that we always read index blocks in forward order so this method is always called in forward order
            openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
        }

        public boolean hasNext()
        {
            try
            {
                return hasNextInternal();
            }
            catch (IOException | IndexOutOfBoundsException e)
            {
                try
                {
                    closeInternal();
                }
                catch (IOException suppressed)
                {
                    e.addSuppressed(suppressed);
                }
                sstable.markSuspect();
                throw new CorruptSSTableException(e, reader.file.getPath());
            }
        }

        public Unfiltered next()
        {
            try
            {
                return nextInternal();
            }
            catch (IOException e)
            {
                try
                {
                    closeInternal();
                }
                catch (IOException suppressed)
                {
                    e.addSuppressed(suppressed);
                }
                sstable.markSuspect();
                throw new CorruptSSTableException(e, reader.file.getPath());
            }
        }

        // Set the reader so its hasNext/next methods return values within the provided slice
        public abstract void setForSlice(Slice slice) throws IOException;

        protected abstract boolean hasNextInternal() throws IOException;
        protected abstract Unfiltered nextInternal() throws IOException;

        public void close() throws IOException
        {
            if (shouldCloseFile && file != null)
                file.close();
        }
    }

    // Reader for when we have Slices.NONE but need to read static row or partition level deletion
    private class NoRowsReader extends AbstractSSTableIterator.Reader
    {
        private NoRowsReader(FileDataInput file, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);
        }

        public void setForSlice(Slice slice) throws IOException
        {
            return;
        }

        protected boolean hasNextInternal() throws IOException
        {
            return false;
        }

        protected Unfiltered nextInternal() throws IOException
        {
            throw new NoSuchElementException();
        }
    }

    // Used by indexed readers to store where they are of the index.
    protected static class IndexState implements AutoCloseable
    {
        private final Reader reader;
        private final ClusteringComparator comparator;

        private final IndexedEntry indexEntry;
        private final boolean reversed;

        private IndexInfo currentIndexInfo;

        // Marks the beginning of the block corresponding to currentIndexIdx.
        private DataPosition mark;

        public IndexState(Reader reader, ClusteringComparator comparator, IndexedEntry indexEntry, boolean reversed)
        {
            this.reader = reader;
            this.comparator = comparator;
            this.indexEntry = indexEntry;
            this.reversed = reversed;
            this.currentIndexInfo = null;
        }

        public void updateStateForStartingBlock(boolean onlyPeek) throws IOException
        {
            if (indexEntry.hasNext())
            {
                IndexInfo indexInfo = indexEntry.next();
                //IndexInfo indexInfo = (onlyPeek) ? indexEntry.peek() : indexEntry.next();
                //IndexInfo indexInfo = indexEntry.peek();
                assert indexInfo != null; // kj remove assert
                currentIndexInfo = indexInfo;
                logger.info("in AbstractSSTableIterator#updateStateForStartingBlock() .. we did have next.. seeking to {} ==> indexEntry.getPosition() {} indexInfo.getOffset() {}", indexEntry.getPosition() + indexInfo.getOffset(), indexEntry.getPosition(), indexInfo.getOffset());
                reader.seekToPosition(indexEntry.getPosition() + indexInfo.getOffset());
                reader.deserializer.clearState();
            }
            else
            {
                assert 1 == 2;
            }

            IndexInfo currentIndexInfoPlusOne = (!indexEntry.hasNext()) ? null : indexEntry.peek();
            //IndexInfo currentIndexInfoPlusOne = indexEntry.peek();
            //assert currentIndexInfoPlusOne != null;
            reader.openMarker = (currentIndexInfoPlusOne != null) ? currentIndexInfoPlusOne.endOpenMarker : null;
            mark = reader.file.mark();
        }

        public int blocksCount()
        {
            return indexEntry.entryCount();
        }

        // Update the block idx based on the current reader position if we're past the current block.
        // This only makes sense for forward iteration (for reverse ones, when we reach the end of a block we
        // should seek to the previous one, not update the index state and continue).
        public void updateBlock() throws IOException
        {
            assert !reversed;

            //if (!indexEntry.hasNext())
            if (1 == 2)
                return;

            while (indexEntry.hasNext() && isPastCurrentBlock())
            {
                reader.openMarker = currentIndex().endOpenMarker;
                currentIndexInfo = indexEntry.next();

                // We have to set the mark, and we have to set it at the beginning of the block. So if we're not at the beginning of the block, this forces us to a weird seek dance.
                // This can only happen when reading old file however.
                long startOfBlock = indexEntry.getPosition() + currentIndex().getOffset();
                logger.info("kjkjkj AbstractSSTableIterator#updateBlock() startOfBlock: {} indexEntry.getPosition(): {} currentIndex().getOffset(): {}", startOfBlock, indexEntry.getPosition(), currentIndex().getOffset());
                long currentFilePointer = reader.file.getFilePointer();
                if (startOfBlock == currentFilePointer)
                {
                    mark = reader.file.mark();
                }
                else
                {
                    reader.seekToPosition(startOfBlock);
                    mark = reader.file.mark();
                    reader.seekToPosition(currentFilePointer);
                }
            }
        }

        // Check if we've crossed an index boundary (based on the mark on the beginning of the index block).
        public boolean isPastCurrentBlock()
        {
            assert reader.deserializer != null;
            assert mark != null;
            boolean isPastCurrentBlock = reader.file.bytesPastMark(mark) >= currentIndex().getWidth();
            logger.info("isPastCurrentBlock() ==> {}", isPastCurrentBlock);
            return isPastCurrentBlock;
        }

        public IndexInfo currentIndex()
        {
            return currentIndexInfo;
        }

        @Override
        public String toString()
        {
            return String.format("IndexState(indexSize=%d, currentBlock=%d, reversed=%b)", indexEntry.entryCount(), currentIndexInfo, reversed);
        }

        @Override
        public void close() throws IOException
        {
            indexEntry.close();
        }
    }
}
