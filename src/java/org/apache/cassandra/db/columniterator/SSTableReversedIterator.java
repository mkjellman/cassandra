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
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.btree.BTree;

/**
 *  A Cell Iterator in reversed clustering order over SSTable
 */
public class SSTableReversedIterator extends AbstractSSTableIterator
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableReversedIterator.class);

    /**
     * The index of the slice being processed.
     */
    private int slice;

    public SSTableReversedIterator(SSTableReader sstable,
                                   FileDataInput file,
                                   DecoratedKey key,
                                   IndexedEntry indexEntry,
                                   Slices slices,
                                   ColumnFilter columns)
    {
        super(sstable, file, key, indexEntry, slices, columns);
    }

    protected Reader createReaderInternal(IndexedEntry indexEntry, FileDataInput file, boolean shouldCloseFile)
    {
        return indexEntry.isIndexed()
             ? new ReverseIndexedReader(indexEntry, file, shouldCloseFile)
             : new ReverseReader(file, shouldCloseFile);
    }

    public boolean isReverseOrder()
    {
        return true;
    }

    protected int nextSliceIndex()
    {
        int next = slice;
        slice++;
        return slices.size() - (next + 1);
    }

    protected boolean hasMoreSlices()
    {
        return slice < slices.size();
    }

    private class ReverseReader extends Reader
    {
        protected ReusablePartitionData buffer;
        protected Iterator<Unfiltered> iterator;

        // Set in loadFromDisk () and used in setIterator to handle range tombstone extending on multiple index block. See
        // loadFromDisk for details. Note that those are always false for non-indexed readers.
        protected boolean skipFirstIteratedItem;
        protected boolean skipLastIteratedItem;

        private ReverseReader(FileDataInput file, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);
        }

        protected ReusablePartitionData createBuffer(int blocksCount)
        {
            int estimatedRowCount = 16;
            int columnCount = metadata().regularColumns().size();
            if (columnCount == 0 || metadata().clusteringColumns().isEmpty())
            {
                estimatedRowCount = 1;
            }
            else
            {
                try
                {
                    // To avoid wasted resizing we guess-estimate the number of rows we're likely to read. For that
                    // we use the stats on the number of rows per partition for that sstable.
                    // FIXME: so far we only keep stats on cells, so to get a rough estimate on the number of rows,
                    // we divide by the number of regular columns the table has. We should fix once we collect the
                    // stats on rows
                    int estimatedRowsPerPartition = (int)(sstable.getEstimatedColumnCount().percentile(0.75) / columnCount);
                    estimatedRowCount = Math.max(estimatedRowsPerPartition / blocksCount, 1);
                }
                catch (IllegalStateException e)
                {
                    // The EstimatedHistogram mean() method can throw this (if it overflows). While such overflow
                    // shouldn't happen, it's not worth taking the risk of letting the exception bubble up.
                }
            }
            return new ReusablePartitionData(metadata(), partitionKey(), columns(), estimatedRowCount);
        }

        public void setForSlice(Slice slice) throws IOException
        {
            // If we have read the data, just create the iterator for the slice. Otherwise, read the data.
            if (buffer == null)
            {
                buffer = createBuffer(1);
                // Note that we can reuse that buffer between slices (we could alternatively re-read from disk
                // every time, but that feels more wasteful) so we want to include everything from the beginning.
                // We can stop at the slice end however since any following slice will be before that.
                loadFromDisk(null, slice.end(), false, false);
            }
            setIterator(slice);
        }

        protected void setIterator(Slice slice)
        {
            assert buffer != null;
            iterator = buffer.built.unfilteredIterator(columns, Slices.with(metadata().comparator, slice), true);

            if (!iterator.hasNext())
                return;

            if (skipFirstIteratedItem)
                iterator.next();

            if (skipLastIteratedItem)
                iterator = new SkipLastIterator(iterator);
        }

        protected boolean hasNextInternal() throws IOException
        {
            // If we've never called setForSlice, we're reading everything
            if (iterator == null)
                setForSlice(Slice.ALL);

            return iterator.hasNext();
        }

        protected Unfiltered nextInternal() throws IOException
        {
            if (!hasNext())
                throw new NoSuchElementException();
            return iterator.next();
        }

        protected boolean stopReadingDisk()
        {
            return false;
        }

        // Reads the unfiltered from disk and load them into the reader buffer. It stops reading when either the partition
        // is fully read, or when stopReadingDisk() returns true.
        protected void loadFromDisk(ClusteringBound start,
                                    ClusteringBound end,
                                    boolean hasPreviousBlock,
                                    boolean hasNextBlock) throws IOException
        {
            logger.info("SSTableReversedIterator#loadFromDisk start null? {} end null? {} hasPreviousBlock? {} hasNextBlock? {} openMarker null? {}", start == null, end == null, hasPreviousBlock, hasNextBlock, openMarker == null);
            
            // start != null means it's the block covering the beginning of the slice, so it has to be the last block for this slice.
            assert start == null || !hasNextBlock;

            buffer.reset();
            skipFirstIteratedItem = false;
            skipLastIteratedItem = false;

            // If the start might be in this block, skip everything that comes before it.
            if (start != null)
            {
                while (deserializer.hasNext() && deserializer.compareNextTo(start) <= 0 && !stopReadingDisk())
                {
                    if (deserializer.nextIsRow())
                        deserializer.skipNext();
                    else
                        updateOpenMarker((RangeTombstoneMarker)deserializer.readNext());
                }
            }

            // If we have an open marker, it's either one from what we just skipped or it's one that open in the next (or
            // one of the next) index block (if openMarker == openMarkerAtStartOfBlock).
            if (openMarker != null)
            {
                // We have to feed a marker to the buffer, because that marker is likely to be close later and ImmtableBTreePartition
                // doesn't take kindly to marker that comes without their counterpart. If that's the last block we're gonna read (for
                // the current slice at least) it's easy because we'll want to return that open marker at the end of the data in this
                // block anyway, so we have nothing more to do than adding it to the buffer.
                // If it's not the last block however, in which case we know we'll have start == null, it means this marker is really
                // open in a next block and so while we do need to add it the buffer for the reason mentioned above, we don't
                // want to "return" it just yet, we'll wait until we reach it in the next blocks. That's why we trigger
                // skipLastIteratedItem in that case (this is first item of the block, but we're iterating in reverse order
                // so it will be last returned by the iterator).

                logger.info("openMarker [1/2] != null...");

                ClusteringBound markerStart = start == null ? ClusteringBound.BOTTOM : start;
                buffer.add(new RangeTombstoneBoundMarker(markerStart, openMarker));
                if (hasNextBlock)
                    skipLastIteratedItem = true;
            }

            // Now deserialize everything until we reach our requested end (if we have one)
            // See SSTableIterator.ForwardRead.computeNext() for why this is a strict inequality below: this is the same
            // reasoning here.
            while (deserializer.hasNext()
                   && (end == null || deserializer.compareNextTo(end) < 0)
                   && !stopReadingDisk())
            {
                Unfiltered unfiltered = deserializer.readNext();
                // We may get empty row for the same reason expressed on UnfilteredSerializer.deserializeOne.
                if (!unfiltered.isEmpty())
                    buffer.add(unfiltered);

                if (unfiltered.isRangeTombstoneMarker())
                    updateOpenMarker((RangeTombstoneMarker)unfiltered);
            }

            // If we have an open marker, we should close it before finishing
            if (openMarker != null)
            {
                // This is the reverse problem than the one at the start of the block. Namely, if it's the first block
                // we deserialize for the slice (the one covering the slice end basically), then it's easy, we just want
                // to add the close marker to the buffer and return it normally.
                // If it's note our first block (for the slice) however, it means that marker closed in a previously read
                // block and we have already returned it. So while we should still add it to the buffer for the sake of
                // not breaking ImmutableBTreePartition, we should skip it when returning from the iterator, hence the
                // skipFirstIteratedItem (this is the last item of the block, but we're iterating in reverse order so it will
                // be the first returned by the iterator).

                logger.info("openMarker [2/2] != null...");

                ClusteringBound markerEnd = end == null ? ClusteringBound.TOP : end;
                buffer.add(new RangeTombstoneBoundMarker(markerEnd, openMarker));
                if (hasPreviousBlock)
                    skipFirstIteratedItem = true;
            }

            buffer.build();
        }
    }

    private class ReverseIndexedReader extends ReverseReader
    {
        private final IndexState indexState;

        // The slice we're currently iterating over
        private Slice slice;
        private final IndexedEntry indexedEntry;

        private ReverseIndexedReader(IndexedEntry indexEntry, FileDataInput file, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);
            this.indexState = new IndexState(this, metadata.comparator, indexEntry, true);
            this.indexedEntry = indexEntry;
        }

        @Override
        public void close() throws IOException
        {
            super.close();
            this.indexState.close();
        }

        @Override
        public void setForSlice(Slice slice) throws IOException
        {
            this.slice = slice;

            // if our previous slicing already got us past the beginning of the sstable, we're done
            if (!indexedEntry.hasNext())
            {
                iterator = Collections.emptyIterator();
                return;
            }

            // Find the first index block we'll need to read for the slice.
            //indexedEntry.setIteratorBounds(null, slice.start(), metadata.comparator, true);
            //indexedEntry.setIteratorBounds(slice.end(), slice.start(), true);
            indexedEntry.setIteratorBounds(slice.start(), slice.end(), true);
            if (!indexedEntry.hasNext())
            {
                iterator = Collections.emptyIterator();
                return;
            }

            // Note that even if we were already set on the proper block (which would happen if the previous slice
            // requested ended on the same block this one start), we can't reuse it because when reading the previous
            // slice we've only read that block from the previous slice start. Re-reading also handles
            // skipFirstIteratedItem/skipLastIteratedItem that we would need to handle otherwise.
            //indexState.setToBlock(startIdx); //todo kj: need to handle this logic added for CASSANDRA-13340

            indexState.updateStateForStartingBlock(true);
            readCurrentBlock(false, indexedEntry.hasNext());
        }

        @Override
        protected boolean hasNextInternal() throws IOException
        {
            if (super.hasNextInternal())
                return true;

            if (!indexedEntry.hasNext())
            {
                return false;
            }

            // todo kj: this was changed for CASSANDRA-13340, need to see impact to my changes
            // The slice start can be in
            //indexState.setToBlock(nextBlockIdx);
            //readCurrentBlock(true, nextBlockIdx != lastBlockIdx);

            indexState.updateStateForStartingBlock(false);
            readCurrentBlock(true, indexedEntry.hasNext());

            // since that new block is within the bounds we've computed in setToSlice(), we know there will
            // always be something matching the slice unless we're on the lastBlockIdx (in which case there
            // may or may not be results, but if there isn't, we're done for the slice).
            return iterator.hasNext();
        }

        /**
         * Reads the current block, the last one we've set.
         *
         * @param hasPreviousBlock is whether we have already read a previous block for the current slice.
         * @param hasNextBlock is whether we have more blocks to read for the current slice.
         */
        private void readCurrentBlock(boolean hasPreviousBlock, boolean hasNextBlock) throws IOException
        {
            if (buffer == null)
                buffer = createBuffer(indexState.blocksCount());

            // todo kj: this was changed for CASSANDRA-13340, need to see impact to my changes
            // The slice start (resp. slice end) is only meaningful on the last (resp. first) block read (since again,
            // we read blocks in reverse order).
            boolean canIncludeSliceStart = !hasNextBlock;
            boolean canIncludeSliceEnd = !hasPreviousBlock;
            logger.info("readCurrentBlock: canIncludeSliceStart: {} hasNextBlock: {} canIncludeSliceEnd: {} hasPreviousBlock: {} slice.start() == null? {}", canIncludeSliceStart, hasNextBlock, canIncludeSliceEnd, hasPreviousBlock, slice.start() == null);

            loadFromDisk(canIncludeSliceStart ? slice.start() : null,
                         canIncludeSliceEnd ? slice.end() : null,
                         hasPreviousBlock,
                         hasNextBlock);
            setIterator(slice);
        }

        @Override
        protected boolean stopReadingDisk()
        {
            return indexState.isPastCurrentBlock();
        }
    }

    private class ReusablePartitionData
    {
        private final TableMetadata metadata;
        private final DecoratedKey partitionKey;
        private final RegularAndStaticColumns columns;

        private MutableDeletionInfo.Builder deletionBuilder;
        private MutableDeletionInfo deletionInfo;
        private BTree.Builder<Row> rowBuilder;
        private ImmutableBTreePartition built;

        private ReusablePartitionData(TableMetadata metadata,
                                      DecoratedKey partitionKey,
                                      RegularAndStaticColumns columns,
                                      int initialRowCapacity)
        {
            this.metadata = metadata;
            this.partitionKey = partitionKey;
            this.columns = columns;
            this.rowBuilder = BTree.builder(metadata.comparator, initialRowCapacity);
        }


        public void add(Unfiltered unfiltered)
        {
            if (unfiltered.isRow())
                rowBuilder.add((Row)unfiltered);
            else
                deletionBuilder.add((RangeTombstoneMarker)unfiltered);
        }

        public void reset()
        {
            built = null;
            rowBuilder = BTree.builder(metadata.comparator);
            deletionBuilder = MutableDeletionInfo.builder(partitionLevelDeletion, metadata().comparator, false);
            //deletionBuilder = MutableDeletionInfo.builder(partitionLevelDeletion, metadata().comparator, true);
            // kjellman we really need to fix this it's so lame that false on a reversed iterator leads to the correct behavior
        }

        public void build()
        {
            deletionInfo = deletionBuilder.build();
            built = new ImmutableBTreePartition(metadata, partitionKey, columns, Rows.EMPTY_STATIC_ROW, rowBuilder.build(),
                                                deletionInfo, EncodingStats.NO_STATS);
            deletionBuilder = null;
        }
    }

    private static class SkipLastIterator extends AbstractIterator<Unfiltered>
    {
        private final Iterator<Unfiltered> iterator;

        private SkipLastIterator(Iterator<Unfiltered> iterator)
        {
            this.iterator = iterator;
        }

        protected Unfiltered computeNext()
        {
            if (!iterator.hasNext())
                return endOfData();

            Unfiltered next = iterator.next();
            return iterator.hasNext() ? next : endOfData();
        }
    }
}
