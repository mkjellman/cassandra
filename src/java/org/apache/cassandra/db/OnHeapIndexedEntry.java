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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import com.google.common.primitives.Ints;

import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * IndexedEntry implementation of On-Heap IndexInfo (non B+(ish) Tree based).
 * At a high level IndexInfo objects are serialized and deserialized into a List
 * and then binary searched over to find the offset+width for a given indexed key.
 */
public class OnHeapIndexedEntry implements IndexedEntry
{
    private static final long BASE_SIZE =
    ObjectSizes.measure(new OnHeapIndexedEntry(0, DeletionTime.LIVE, 0, Arrays.<IndexInfo>asList(null, null), null))
    + ObjectSizes.measure(new ArrayList<>(1));

    private final long position;
    private final long headerLength; // The offset in the file when the index entry end
    private final List<IndexInfo> columnsIndex;
    private final DeletionTime deletionTime;
    private final FileDataInput reader;

    private int lastDeserializedBlock = -1;
    private int lastBlockToIterateTo = -1;
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
        this.lastBlockToIterateTo = columnsIndex.size();
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

    public IndexInfo getIndexInfo(ClusteringPrefix name, ClusteringComparator comparator, boolean reversed) throws IOException
    {
        int indexIdx = indexFor(name, comparator);
        if (indexIdx < 0 || indexIdx >= columnsIndex.size())
        {
            // no index block for that slice
            return null; //todo: returning null is lame
        }

        return columnsIndex.get(indexIdx);
    }

    private int getNextIndexIdxIfIterated()
    {
        int calculatedNextIndexIdx = lastDeserializedBlock;

        if (iteratorDirectionReversed)
            calculatedNextIndexIdx--;
        else
            calculatedNextIndexIdx++;
        return calculatedNextIndexIdx;
    }

    public boolean hasNext()
    {

        int calculatedNextIndexIdx = getNextIndexIdxIfIterated();
        return iteratorDirectionReversed ? calculatedNextIndexIdx > lastBlockToIterateTo
                                         : calculatedNextIndexIdx < lastBlockToIterateTo;
    }

    public IndexInfo next()
    {
        int nextIdx = getNextIndexIdxIfIterated();
        isIdxWithinValidBounds(nextIdx);

        if (iteratorDirectionReversed)
            lastDeserializedBlock--;
        else
            lastDeserializedBlock++;

        return (lastDeserializedBlock < columnsIndex.size() && lastDeserializedBlock >= 0) ? columnsIndex.get(lastDeserializedBlock) : null;
    }

    public IndexInfo peek()
    {
        int calculatedNextIndexIdx = getNextIndexIdxIfIterated();
        if (iteratorDirectionReversed)
            return (calculatedNextIndexIdx < columnsIndex.size() && calculatedNextIndexIdx >= 0) ? columnsIndex.get(calculatedNextIndexIdx) : null;
        else
            return (calculatedNextIndexIdx < columnsIndex.size()) ? columnsIndex.get(calculatedNextIndexIdx) : null;
    }

    /**
     * Single place to check if a given idx is actually "valid"
     * given the number of elements backing the columnsIndex array.
     * @param idx index for a given column index element to validate
     */
    private void isIdxWithinValidBounds(int idx)
    {
        // we want to use columnsIndex.size() here and not lastBlockToIterateTo as most likely
        // we're checking this condition when we are in the process of trying to set lastBlockToIterateTo
        // in the first place
        assert idx >= -1 && idx <= columnsIndex.size() : String.format("idx: %d not within valid bounds " +
                                                                       "%d <--> %d", idx, -1, columnsIndex.size());
    }

    public void setIteratorBounds(ClusteringBound start, ClusteringBound end, ClusteringComparator comparator, boolean reversed) throws IOException
    {
        iteratorDirectionReversed = reversed;

        int nextStartIdx;
        if (start == ClusteringBound.BOTTOM)
        {
            nextStartIdx = -1;
        }
        else if (start == ClusteringBound.TOP)
        {
            nextStartIdx = entryCount();
        }
        else
        {
            nextStartIdx = indexFor(start, comparator);
            nextStartIdx = (reversed) ? nextStartIdx + 1 : nextStartIdx - 1;
        }

        isIdxWithinValidBounds(nextStartIdx);
        lastDeserializedBlock = nextStartIdx;

        int lastBlockIdx;
        if (end == ClusteringBound.BOTTOM)
        {
            lastBlockIdx = -1;
        }
        else if (end == ClusteringBound.TOP)
        {
            lastBlockIdx = entryCount();
        }
        else
        {
            lastBlockIdx = indexFor(end, comparator);
            if (lastBlockIdx >= 0 && lastBlockIdx < entryCount())
                lastBlockIdx = (reversed) ? lastBlockIdx - 1 : lastBlockIdx + 1;
        }
        isIdxWithinValidBounds(lastBlockIdx);
        lastBlockToIterateTo = lastBlockIdx;
    }

    public void close()
    {
        if (reader != null)
            FileUtils.closeQuietly(reader);
    }

    public void reset(boolean reversed)
    {
        this.iteratorDirectionReversed = reversed;
        this.lastBlockToIterateTo = reversed ? -1 : columnsIndex.size();
        this.lastDeserializedBlock = reversed ? columnsIndex.size() : -1;
    }

    public boolean isReversed()
    {
        return iteratorDirectionReversed;
    }

    public void setReversed(boolean reversed)
    {
        // todo: kjellman -- i'm torn behind having this literally set the reversed boolean
        // or call reset (which will set it too) -- given how complicated the state logic is
        // i feel like any time the iterator direction is changed we should always just go to
        // the defaults (at minimum) for start and end offsets
        reset(reversed);
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

    private int indexFor(ClusteringPrefix name, ClusteringComparator comparator) throws IOException
    {
        IndexInfo target = new IndexInfo(name, name, 0, 0, null);
        /*
        Take the example from the unit test, and say your index looks like this:
        [0..5][10..15][20..25]
        and you look for the slice [13..17].

        When doing forward slice, we are doing a binary search comparing 13 (the start of the query)
        to the lastName part of the index slot. You'll end up with the "first" slot, going from left to right,
        that may contain the start.

        When doing a reverse slice, we do the same thing, only using as a start column the end of the query,
        i.e. 17 in this example, compared to the firstName part of the index slots.  bsearch will give us the
        first slot where firstName > start ([20..25] here), so we subtract an extra one to get the slot just before.
        */
        int startIdx = 0;
        int endIdx = entryCount() - 1;

        if (iteratorDirectionReversed)
        {
            if (lastBlockToIterateTo < endIdx)
            {
                endIdx = lastBlockToIterateTo;
            }
        }
        else
        {
            if (lastDeserializedBlock > 0)
            {
                startIdx = lastDeserializedBlock;
            }
        }

        int index = binarySearch(target, comparator.indexComparator(iteratorDirectionReversed), startIdx, endIdx);
        return (index < 0 ? -index - (iteratorDirectionReversed ? 2 : 1) : index);
    }

    private int binarySearch(IndexInfo key, Comparator<IndexInfo> c, int low, int high)
    {
        while (low <= high)
        {
            int mid = (low + high) >>> 1;
            IndexInfo midVal = columnsIndex.get(mid);
            int cmp = c.compare(midVal, key);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }
}
