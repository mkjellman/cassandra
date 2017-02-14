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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * Created by mkjellman on 1/12/17.
 */
public class OnHeapIndexedEntry implements IndexedEntry
{
    private static final Logger logger = LoggerFactory.getLogger(OnHeapIndexedEntry.class);

    private static final long BASE_SIZE =
    ObjectSizes.measure(new OnHeapIndexedEntry(0, DeletionTime.LIVE, 0, Arrays.<IndexInfo>asList(null, null), null))
    + ObjectSizes.measure(new ArrayList<>(1));

    private final long position;
    private final long headerLength; // The offset in the file when the index entry end
    private final List<IndexInfo> columnsIndex;
    private final DeletionTime deletionTime;
    private final FileDataInput reader;

    private int nextIndexIdx = -1;
    private int lastDeserializedBlock = -1;
    private boolean iteratorDirectionReversed = false;

    public OnHeapIndexedEntry(long position, DeletionTime deletionTime, long headerLength, List<IndexInfo> columnsIndex, FileDataInput reader)
    {
        assert deletionTime != null;
        assert columnsIndex != null && columnsIndex.size() > 1;

        this.position = position;
        this.deletionTime = deletionTime;
        this.headerLength = headerLength;
        this.columnsIndex = columnsIndex;
        this.reader = reader;
    }

    public long getPosition()
    {
        return position;
    }

    public boolean isIndexed()
    {
        return true;
    }

    public DeletionTime deletionTime()
    {
        return deletionTime;
    }

    public long headerLength()
    {
        return headerLength;
    }

    public List<IndexInfo> getAllColumnIndexes()
    {
        return columnsIndex;
    }

    public int entryCount()
    {
        return columnsIndex.size();
    }

    public IndexInfo getIndexInfo(ClusteringPrefix name, ClusteringComparator comparator, boolean reversed)
    {
        iteratorDirectionReversed = reversed;
        nextIndexIdx = indexFor(name, comparator);

        if (nextIndexIdx < 0 || nextIndexIdx >= columnsIndex.size())
        {
            // no index block for that slice
            return null; //todo: returning null is lame
        }

        return columnsIndex.get(nextIndexIdx);
    }

    private int getNextIndexIdxIfIterated()
    {
        int calculatedNextIndexIdx = nextIndexIdx;
        if (lastDeserializedBlock == calculatedNextIndexIdx)
        {
            logger.info("getNextIndexIdxIfIterated lastDeserializedBlock: {} calculatedNextIndexIdx: {}", lastDeserializedBlock, calculatedNextIndexIdx);
            if (iteratorDirectionReversed)
                calculatedNextIndexIdx--;
            else
                calculatedNextIndexIdx++;
        }

        return calculatedNextIndexIdx;
    }

    public boolean hasNext()
    {
        int calculatedNextIndexIdx = getNextIndexIdxIfIterated();
        logger.info("do we have next? {} => in hasNext nextIndexIdx: {} columnsIndex.size(): {}", (calculatedNextIndexIdx >= 0 && calculatedNextIndexIdx < columnsIndex.size()) ? "YES" : "NO", calculatedNextIndexIdx, columnsIndex.size());
        return iteratorDirectionReversed ? calculatedNextIndexIdx >= 0 : calculatedNextIndexIdx <= columnsIndex.size();
    }

    public IndexInfo next()
    {
        if (lastDeserializedBlock == nextIndexIdx)
        {
            logger.info("yes, lastDeserializedBlock {} == nextIndexIdx {}", lastDeserializedBlock, nextIndexIdx);
            if (iteratorDirectionReversed)
                nextIndexIdx--;
            else
            nextIndexIdx++;
        }

        logger.info("next() called => updating lastDeserializedBlock from {} to {}.... reversed? {}", lastDeserializedBlock, nextIndexIdx, iteratorDirectionReversed);
        lastDeserializedBlock = nextIndexIdx;
        if (iteratorDirectionReversed)
            nextIndexIdx--;
        else
            nextIndexIdx++;

        return (lastDeserializedBlock < columnsIndex.size() && lastDeserializedBlock >= 0) ? columnsIndex.get(lastDeserializedBlock) : null;
    }

    public IndexInfo peek()
    {
        int calculatedNextIndexIdx = getNextIndexIdxIfIterated();
        logger.info("calculatedNextIndexIdx: {}", calculatedNextIndexIdx);
        if (iteratorDirectionReversed)
            return (calculatedNextIndexIdx >= 0) ? columnsIndex.get(calculatedNextIndexIdx) : null;
        else
            return (calculatedNextIndexIdx < columnsIndex.size()) ? columnsIndex.get(calculatedNextIndexIdx) : null;
    }

    public void startIteratorAt(ClusteringPrefix name, ClusteringComparator comparator, boolean reversed)
    {
        lastDeserializedBlock = -1;
        iteratorDirectionReversed = reversed;
        nextIndexIdx = indexFor(name, comparator);
    }

    public void close()
    {
        if (reader != null)
            FileUtils.closeQuietly(reader);
    }

    public void reset(boolean reversed)
    {
        iteratorDirectionReversed = reversed;
        lastDeserializedBlock = -1;
        nextIndexIdx = -1;
    }

    public boolean isReversed()
    {
        return iteratorDirectionReversed;
    }

    public int promotedSize(IndexInfo.Serializer idxSerializer)
    {
        long size = TypeSizes.sizeofUnsignedVInt(headerLength)
                    + DeletionTime.serializer.serializedSize(deletionTime)
                    + TypeSizes.sizeofUnsignedVInt(columnsIndex.size()); // number of entries
        for (IndexInfo info : columnsIndex)
            size += idxSerializer.serializedSize(info);

        size += columnsIndex.size() * TypeSizes.sizeof(0);

        return Ints.checkedCast(size);
    }

    @Override
    public long unsharedHeapSize()
    {
        long entrySize = 0;
        for (IndexInfo idx : columnsIndex)
            entrySize += idx.unsharedHeapSize();

        return BASE_SIZE
               + entrySize
               + deletionTime.unsharedHeapSize()
               + ObjectSizes.sizeOfReferenceArray(columnsIndex.size());
    }

    /**
     * The index of the IndexInfo in which a scan starting with @name should begin.
     *
     * @param name name to search for
     * @param comparator the comparator to use
     *
     * @return int index
     */
    private int indexFor(ClusteringPrefix name, ClusteringComparator comparator)
    {
        logger.info("kj123 lastDeserializedBlock: {} nextIndexIdx: {} columnsIndex.size(): {} reversed? {}", lastDeserializedBlock, nextIndexIdx, columnsIndex.size(), iteratorDirectionReversed);

        IndexInfo target = new IndexInfo(name, name, 0, 0, null);
        /*
        Take the example from the unit test, and say your index looks like this:
        [0..5][10..15][20..25]
        and you look for the slice [13..17].

        When doing forward slice, we we doing a binary search comparing 13 (the start of the query)
        to the lastName part of the index slot. You'll end up with the "first" slot, going from left to right,
        that may contain the start.

        When doing a reverse slice, we do the same thing, only using as a start column the end of the query,
        i.e. 17 in this example, compared to the firstName part of the index slots.  bsearch will give us the
        first slot where firstName > start ([20..25] here), so we subtract an extra one to get the slot just before.
        */
        int startIdx = 0;
        List<IndexInfo> toSearch = columnsIndex;
        if (iteratorDirectionReversed && lastDeserializedBlock >= 0)
        {
            toSearch = columnsIndex.subList(0, lastDeserializedBlock + 1);
        }
        else if (!iteratorDirectionReversed && lastDeserializedBlock > 0)
        {
            // in the paging case we could have [0-100][101-200][201-300][401-500] with a paging size of 100.
            // If the IndexInfo covering the [101-200] range actually also covers data for some of the [201-300]
            // range, then the next page request also needs to consider the previous IndexInfo; otherwise
            // we'd incorrectly filter out the last returned index.
            startIdx = lastDeserializedBlock - 1;
            toSearch = columnsIndex.subList(lastDeserializedBlock - 1, columnsIndex.size());
        }

        int index = Collections.binarySearch(toSearch, target, comparator.indexComparator(iteratorDirectionReversed));
        logger.info("binary search returned {} => startIdx: {}, reversed: {} toSearch.size: {} columnsIndex.size: {}", index, startIdx, iteratorDirectionReversed, toSearch.size(), columnsIndex.size());
        return startIdx + (index < 0 ? -index - (iteratorDirectionReversed ? 2 : 1) : index);
    }
}
