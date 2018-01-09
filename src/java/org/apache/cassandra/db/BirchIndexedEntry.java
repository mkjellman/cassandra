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

package org.apache.cassandra.db;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.birch.BirchReader;
import org.apache.cassandra.io.util.PageAlignedReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;

/**
 * An IndexedEntry implementation that is backed by a BirchWriter Index
 */
public class BirchIndexedEntry implements IndexedEntry, AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(BirchIndexedEntry.class);

    private final long position;
    private final TableMetadata tableMetadata;
    private final SerializationHeader header;
    private PageAlignedReader reader;
    private final BirchReader<IndexInfo> birchReader;
    private final long headerLength;
    private final DeletionTime deletionTime;
    private final int readerSegmentIdx;
    private final short readerSubSegmentIdx;

    private boolean prepared;
    private boolean isDone;

    private BirchReader<IndexInfo>.BirchIterator iterator = null;
    private boolean iteratorDirectionReversed = false;

    public BirchIndexedEntry(long position, TableMetadata tableMetadata, SerializationHeader header, Version version,
                             PageAlignedReader reader, long headerLength, DeletionTime deletionTime) throws IOException
    {
        logger.debug("new BirchIndexedEntry created! for position: {}", position);
        this.position = position;
        this.tableMetadata = tableMetadata;
        this.header = header;
        this.reader = reader;
        this.headerLength = headerLength;
        this.deletionTime = deletionTime;
        this.birchReader = new BirchReader<>(reader, header, version);
        this.readerSegmentIdx = reader.getCurrentSegmentIdx();
        this.readerSubSegmentIdx = reader.getCurrentSubSegmentIdx();
    }

    public boolean isIndexed()
    {
        return true;
    }

    public long headerLength()
    {
        return headerLength;
    }

    public DeletionTime deletionTime()
    {
        return deletionTime;
    }

    public int promotedSize(ClusteringComparator comparator)
    {
        return 0;
    }

    @Override
    public long unsharedHeapSize()
    {
        return 0;
    }

    public long getPosition()
    {
        return position;
    }

    public int entryCount()
    {
        return birchReader.getElementCount();
    }

    public int promotedSize(IndexInfo.Serializer type)
    {
        return 0;
    }

    public List<IndexInfo> getAllColumnIndexes()
    {
        throw new UnsupportedOperationException();
    }

    public IndexInfo getIndexInfo(ClusteringPrefix name, boolean reversed) throws IOException
    {
        assert reader.isCurrentSubSegmentPageAligned()
               && reader.getCurrentSegmentIdx() == readerSegmentIdx;

        iteratorDirectionReversed = reversed;
        return birchReader.search(name, tableMetadata, reversed);
    }

    public boolean hasNext()
    {
        assert reader.getCurrentSegmentIdx() == readerSegmentIdx; //kjkjk add me back

        //assert iterator != null;
        /*
        if (!prepared && !isDone)
            return true;
        if (prepared && isDone)
            return false;
        if (iterator != null)
        {
            boolean hasNext = iterator.hasNext();
            if (!hasNext)
            {
                isDone = true;
                return false;
            }
            else
            {
                return true;
            }
        }
        return false;
        */
        boolean hasNext = !prepared || (iterator != null && iterator.hasNext());
        if (hasNext)
            assert !isDone;
        logger.debug("BirchIndexedEntry#hasNext {}", hasNext);
        return hasNext;
        //return iterator == null || iterator.hasNext();
    }

    public IndexInfo next()
    {
        assert reader.getCurrentSegmentIdx() == readerSegmentIdx;
        if (iterator == null)
            return null;
        IndexInfo next = iterator.next();
        //next.setEndOpenMarker(deletionTime);
        return next;
        //return (iterator != null) ? iterator.next() : null;
    }

    public IndexInfo peek()
    {
        return (iterator != null) ? iterator.peek() : null;
    }

    public void setIteratorBounds(ClusteringBound start, ClusteringBound end, boolean reversed) throws IOException
    {
        assert reader.getCurrentSegmentIdx() == readerSegmentIdx;

        logger.info("setIteratorBounds start: {} end: {}", start.toString(tableMetadata), end.toString(tableMetadata));

        iteratorDirectionReversed = reversed;
        iterator = birchReader.getIterator(start, end, tableMetadata, reversed);
        prepared = true;

        // todo kjellman: need to switch getIterator to new start/end in one go method
    }

    public boolean isReversed()
    {
        return iteratorDirectionReversed;
    }

    public void setReversed(boolean reversed)
    {
        logger.debug("BirchIndexedEntry#setReversed(reversed {})", reversed);
        reset(reversed);
        iteratorDirectionReversed = reversed;
        iterator = null;
        prepared = false;
    }

    @Override
    public void close()
    {
        logger.info("BirchIndexedEntry#close()");
        FileUtils.closeQuietly(birchReader);
        FileUtils.closeQuietly(reader);
    }

    public void reset(boolean reversed)
    {
        logger.debug("BirchIndexedEntry#reset(reversed {})", reversed);
        this.iteratorDirectionReversed = reversed;
        try
        {
            if (reader.isClosed())
            {
                reader = PageAlignedReader.copy(reader);
                birchReader.unsafeReplaceReader(reader);
            }

            // the birch index is always in the 2nd sub-segment
            reader.setSegment(readerSegmentIdx, 1);
            iterator = birchReader.getIterator(tableMetadata, reversed);
            //prepared = true;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        iteratorDirectionReversed = reversed;
    }
}
