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
package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.io.util.PageAlignedReader;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.AbstractBounds.Boundary;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.dht.AbstractBounds.isEmpty;
import static org.apache.cassandra.dht.AbstractBounds.maxLeft;
import static org.apache.cassandra.dht.AbstractBounds.minRight;

public class BigTableScanner implements ISSTableScanner
{
    private static final Logger logger = LoggerFactory.getLogger(BigTableScanner.class);

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    protected final RandomAccessReader dfile;
    private FileDataInput ifile;
    //private final FileDataInput ifile;
    public final SSTableReader sstable;

    private final Iterator<AbstractBounds<PartitionPosition>> rangeIterator;
    private AbstractBounds<PartitionPosition> currentRange;

    private final ColumnFilter columns;
    private final DataRange dataRange;
    private final IndexedEntry.Serializer rowIndexEntrySerializer;
    private final SSTableReadsListener listener;
    private long startScan = -1;
    private long bytesScanned = 0;

    protected Iterator<UnfilteredRowIterator> iterator;

    // Full scan of the sstables
    public static ISSTableScanner getScanner(SSTableReader sstable)
    {
        return getScanner(sstable, Iterators.singletonIterator(fullRange(sstable)));
    }

    public static ISSTableScanner getScanner(SSTableReader sstable,
                                             ColumnFilter columns,
                                             DataRange dataRange,
                                             SSTableReadsListener listener)
    {
        return new BigTableScanner(sstable, columns, dataRange, makeBounds(sstable, dataRange).iterator(), listener);
    }

    public static ISSTableScanner getScanner(SSTableReader sstable, Collection<Range<Token>> tokenRanges)
    {
        // We want to avoid allocating a SSTableScanner if the range don't overlap the sstable (#5249)
        List<Pair<Long, Long>> positions = sstable.getPositionsForRanges(tokenRanges);
        if (positions.isEmpty())
            return new EmptySSTableScanner(sstable);

        return getScanner(sstable, makeBounds(sstable, tokenRanges).iterator());
    }

    public static ISSTableScanner getScanner(SSTableReader sstable, Iterator<AbstractBounds<PartitionPosition>> rangeIterator)
    {
        return new BigTableScanner(sstable, ColumnFilter.all(sstable.metadata()), null, rangeIterator, SSTableReadsListener.NOOP_LISTENER);
    }

    private BigTableScanner(SSTableReader sstable,
                            ColumnFilter columns,
                            DataRange dataRange,
                            Iterator<AbstractBounds<PartitionPosition>> rangeIterator,
                            SSTableReadsListener listener)
    {
        assert sstable != null;

        this.dfile = sstable.openDataReader();
        this.ifile = sstable.openIndexReader();
        this.sstable = sstable;
        this.columns = columns;
        this.dataRange = dataRange;
        this.rowIndexEntrySerializer = sstable.descriptor.version.getSSTableFormat().getIndexSerializer(sstable.metadata(),
                                                                                                        sstable.descriptor.version,
                                                                                                        sstable.header);
        this.rangeIterator = rangeIterator;
        this.listener = listener;
    }

    private static List<AbstractBounds<PartitionPosition>> makeBounds(SSTableReader sstable, Collection<Range<Token>> tokenRanges)
    {
        List<AbstractBounds<PartitionPosition>> boundsList = new ArrayList<>(tokenRanges.size());
        for (Range<Token> range : Range.normalize(tokenRanges))
            addRange(sstable, Range.makeRowRange(range), boundsList);
        return boundsList;
    }

    private static List<AbstractBounds<PartitionPosition>> makeBounds(SSTableReader sstable, DataRange dataRange)
    {
        List<AbstractBounds<PartitionPosition>> boundsList = new ArrayList<>(2);
        addRange(sstable, dataRange.keyRange(), boundsList);
        return boundsList;
    }

    private static AbstractBounds<PartitionPosition> fullRange(SSTableReader sstable)
    {
        return new Bounds<PartitionPosition>(sstable.first, sstable.last);
    }

    private static void addRange(SSTableReader sstable, AbstractBounds<PartitionPosition> requested, List<AbstractBounds<PartitionPosition>> boundsList)
    {
        if (requested instanceof Range && ((Range)requested).isWrapAround())
        {
            if (requested.right.compareTo(sstable.first) >= 0)
            {
                // since we wrap, we must contain the whole sstable prior to stopKey()
                Boundary<PartitionPosition> left = new Boundary<PartitionPosition>(sstable.first, true);
                Boundary<PartitionPosition> right;
                right = requested.rightBoundary();
                right = minRight(right, sstable.last, true);
                if (!isEmpty(left, right))
                    boundsList.add(AbstractBounds.bounds(left, right));
            }
            if (requested.left.compareTo(sstable.last) <= 0)
            {
                // since we wrap, we must contain the whole sstable after dataRange.startKey()
                Boundary<PartitionPosition> right = new Boundary<PartitionPosition>(sstable.last, true);
                Boundary<PartitionPosition> left;
                left = requested.leftBoundary();
                left = maxLeft(left, sstable.first, true);
                if (!isEmpty(left, right))
                    boundsList.add(AbstractBounds.bounds(left, right));
            }
        }
        else
        {
            assert requested.left.compareTo(requested.right) <= 0 || requested.right.isMinimum();
            Boundary<PartitionPosition> left, right;
            left = requested.leftBoundary();
            right = requested.rightBoundary();
            left = maxLeft(left, sstable.first, true);
            // apparently isWrapAround() doesn't count Bounds that extend to the limit (min) as wrapping
            right = requested.right.isMinimum() ? new Boundary<PartitionPosition>(sstable.last, true)
                                                    : minRight(right, sstable.last, true);
            if (!isEmpty(left, right))
                boundsList.add(AbstractBounds.bounds(left, right));
        }
    }

    private void seekToCurrentRangeStart()
    {
        long indexPosition = sstable.getIndexScanPosition(currentRange.left);
        try
        {
            if (ifile instanceof PageAlignedReader)
                ((PageAlignedReader) ifile).findAndSetSegmentAndSubSegmentCurrentForPosition(indexPosition);

            ifile.seek(indexPosition);
            while (!ifile.isEOF())
            {
                indexPosition = ifile.getFilePointer();
                DecoratedKey indexDecoratedKey = sstable.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                if (indexDecoratedKey.compareTo(currentRange.left) > 0 || currentRange.contains(indexDecoratedKey))
                {
                    // Found, just read the dataPosition and seek into index and data files
                    // If the sstable has birch indexes, skip the serialized "is indexed" marker
                    if (sstable.descriptor.version.hasBirchIndexes())
                        ifile.readBoolean();

                    long dataPosition = (sstable.descriptor.version.hasBirchIndexes()) ? ifile.readLong()
                                                                                       : IndexedEntry.Serializer.readPosition(ifile, sstable.descriptor.version);

                    if (ifile instanceof PageAlignedReader)
                        ((PageAlignedReader) ifile).findAndSetSegmentAndSubSegmentCurrentForPosition(indexPosition);

                    ifile.seek(indexPosition);
                    dfile.seek(dataPosition);
                    break;
                }
                else
                {
                    IndexedEntry.Serializer.skip(ifile, sstable.descriptor.version);
                }
            }
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
    }

    public void close()
    {
        try
        {
            if (isClosed.compareAndSet(false, true))
                FileUtils.close(dfile, ifile);
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
    }

    public long getLengthInBytes()
    {
        return dfile.length();
    }

    public long getCurrentPosition()
    {
        return dfile.getFilePointer();
    }

    public long getBytesScanned()
    {
        return bytesScanned;
    }

    public long getCompressedLengthInBytes()
    {
        return sstable.onDiskLength();
    }

    public String getBackingFiles()
    {
        return sstable.toString();
    }


    public TableMetadata metadata()
    {
        return sstable.metadata();
    }

    public boolean hasNext()
    {
        if (iterator == null)
            iterator = createIterator();
        return iterator.hasNext();
    }

    public UnfilteredRowIterator next()
    {
        if (iterator == null)
            iterator = createIterator();
        return iterator.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private Iterator<UnfilteredRowIterator> createIterator()
    {
        this.listener.onScanningStarted(sstable);
        return new KeyScanningIterator();
    }

    protected class KeyScanningIterator extends AbstractIterator<UnfilteredRowIterator> implements AutoCloseable
    {
        private DecoratedKey nextKey;
        private IndexedEntry nextEntry;
        private DecoratedKey currentKey;
        private IndexedEntry currentEntry;
        private long lastDeserialziedPos;
        private DataPosition lastDeserializedMark;

        protected UnfilteredRowIterator computeNext()
        {
            try
            {
                // there are some cases where the iterator will be advanced more than once,
                // so we have to do this check to make sure we're "starting" at the same offset
                // we "last" deserialized for the next iteration.
                if (lastDeserialziedPos > 0)
                {
                    if (ifile instanceof RandomAccessReader)
                    {
                        // handle legacy index format
                        ifile.reset(lastDeserializedMark);
                    }
                    else
                    {
                        if (((PageAlignedReader) ifile).isPositionInsideCurrentSegment(lastDeserialziedPos))
                        {
                            if (((PageAlignedReader) ifile).hasNextSegment())
                            {
                                try
                                {
                                    ((PageAlignedReader) ifile).nextSegment();
                                }
                                catch (Exception e)
                                {
                                    Exception closeE = ((PageAlignedReader) ifile).getCloseException();
                                    if (closeE != null)
                                    {
                                        logger.error("Attempted to call nextSegment() on a closed PageAlignedReader. Originally closed " +
                                                     "by the following stack trace", closeE);
                                    }
                                    else
                                    {
                                        logger.error("Attempted to call nextSegment() on a closed PageAlignedReader but the closeException " +
                                                     "was null");
                                    }
                                    throw e;
                                }
                            }
                        }
                        else
                        {
                            if (((PageAlignedReader) ifile).isClosed())
                            {
                                logger.error("ifile we're working on in BigTableScanner iterator is closed!", ((PageAlignedReader) ifile).getCloseException());
                                //ifile = sstable.openIndexReader();
                            }
                            //ifile.reset(lastDeserializedMark);
                        }
                    }
                }

                if (nextEntry == null)
                {
                    do
                    {
                        if (startScan != -1)
                            bytesScanned += dfile.getFilePointer() - startScan;

                        // we're starting the first range or we just passed the end of the previous range
                        if (!rangeIterator.hasNext())
                        {
                            maybeCloseCurrentIndexEntry();
                            maybeCloseNextIndexEntry();

                            return endOfData();
                        }

                        currentRange = rangeIterator.next();
                        seekToCurrentRangeStart();
                        startScan = dfile.getFilePointer();

                        if (ifile.isEOF())
                        {
                            maybeCloseCurrentIndexEntry();
                            maybeCloseNextIndexEntry();

                            return endOfData();
                        }

                        currentKey = sstable.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                        currentEntry = rowIndexEntrySerializer.deserialize(ifile);

                        lastDeserialziedPos = (ifile instanceof RandomAccessReader) ? ifile.getFilePointer() : ((PageAlignedReader)ifile).getOffset();
                        lastDeserializedMark = ifile.mark();
                    } while (!currentRange.contains(currentKey));
                }
                else
                {
                    // we're in the middle of a range
                    currentKey = nextKey;
                    currentEntry = nextEntry;
                }

                if (ifile.isEOF())
                {
                    maybeCloseNextIndexEntry();

                    nextEntry = null;
                    nextKey = null;
                }
                else
                {
                    if (ifile instanceof PageAlignedReader && ifile.isCurrentSegmentExausted())
                        ((PageAlignedReader)ifile).nextSegment();

                    // todo: kj... i'm using isCurrentSubSegmentPageAligned as a way to differ between
                    // a birch segment and a normal index.. this is pretty lame.. can i do better?
                    if (ifile instanceof PageAlignedReader && ((PageAlignedReader)ifile).isCurrentSubSegmentPageAligned())
                    {
                        lastDeserialziedPos = ((PageAlignedReader)ifile).getOffset();
                        lastDeserializedMark = ifile.mark();
                    }
                    else
                    {
                        // we need the position of the start of the next key, regardless of whether it falls in the current range
                        nextKey = sstable.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                        nextEntry = rowIndexEntrySerializer.deserialize(ifile);
                        lastDeserialziedPos = (ifile instanceof PageAlignedReader) ? ((PageAlignedReader)ifile).getOffset() : ifile.getFilePointer();
                        lastDeserializedMark = ifile.mark();
                    }

                    if (nextKey != null && !currentRange.contains(nextKey))
                    {
                        nextKey = null;
                        nextEntry = null;
                    }
                }

                /*
                 * For a given partition key, we want to avoid hitting the data
                 * file unless we're explicitely asked to. This is important
                 * for PartitionRangeReadCommand#checkCacheFilter.
                 */
                return new LazilyInitializedUnfilteredRowIterator(currentKey)
                {
                    protected UnfilteredRowIterator initializeIterator()
                    {

                        if (startScan != -1)
                            bytesScanned += dfile.getFilePointer() - startScan;

                        try
                        {
                            if (dataRange == null)
                            {
                                dfile.seek(currentEntry.getPosition());
                                startScan = dfile.getFilePointer();
                                ByteBufferUtil.skipShortLength(dfile); // key
                                return SSTableIdentityIterator.create(sstable, dfile, partitionKey());
                            }
                            else
                            {
                                startScan = dfile.getFilePointer();
                            }

                            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(partitionKey());
                            currentEntry.setReversed(filter.isReversed()); // todo kjkj IndexQueryPagingTest good repro if we comment this back out
                            return sstable.iterator(dfile, partitionKey(), currentEntry, filter.getSlices(BigTableScanner.this.metadata()), columns, filter.isReversed());
                        }
                        catch (CorruptSSTableException | IOException e)
                        {
                            sstable.markSuspect();
                            throw new CorruptSSTableException(e, sstable.getFilename());
                        }
                    }
                };
            }
            catch (CorruptSSTableException | IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, sstable.getFilename());
            }
        }

        private void maybeCloseCurrentIndexEntry()
        {
            //if (currentEntry != null)
              //  currentEntry.close();
        }

        private void maybeCloseNextIndexEntry()
        {
            //if (nextEntry != null)
            //    nextEntry.close();
        }

        @Override
        public void close()
        {
            maybeCloseCurrentIndexEntry();
            maybeCloseNextIndexEntry();
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" +
               "dfile=" + dfile +
               " ifile=" + ifile +
               " sstable=" + sstable +
               ")";
    }

    public static class EmptySSTableScanner extends AbstractUnfilteredPartitionIterator implements ISSTableScanner
    {
        private final SSTableReader sstable;

        public EmptySSTableScanner(SSTableReader sstable)
        {
            this.sstable = sstable;
        }

        public long getLengthInBytes()
        {
            return 0;
        }

        public long getCurrentPosition()
        {
            return 0;
        }

        public long getBytesScanned()
        {
            return 0;
        }

        public long getCompressedLengthInBytes()
        {
            return 0;
        }

        public String getBackingFiles()
        {
            return sstable.getFilename();
        }

        public TableMetadata metadata()
        {
            return sstable.metadata();
        }

        public boolean hasNext()
        {
            return false;
        }

        public UnfilteredRowIterator next()
        {
            return null;
        }
    }
}
