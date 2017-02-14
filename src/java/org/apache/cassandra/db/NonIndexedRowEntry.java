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

import java.util.Collections;
import java.util.List;

import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * Created by mkjellman on 1/12/17.
 */
public class NonIndexedRowEntry implements IndexedEntry
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new NonIndexedRowEntry(0, null));

    public final long position;
    private final FileDataInput reader;

    public NonIndexedRowEntry(long position, FileDataInput reader)
    {
        this.position = position;
        this.reader = reader;
    }

    public int promotedSize(IndexInfo.Serializer idxSerializer)
    {
        return 0;
    }

    public boolean isIndexed()
    {
        return false;
    }

    public DeletionTime deletionTime()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the offset to the start of the header information for this row.
     * For some formats this may not be the start of the row.
     */
    public long headerOffset()
    {
        return 0;
    }

    public long headerLength()
    {
        throw new UnsupportedOperationException();
    }

    public List<IndexInfo> getAllColumnIndexes()
    {
        return Collections.emptyList();
    }

    public long getPosition()
    {
        return position;
    }
    public int entryCount()
    {
        return 0;
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE;
    }

    public IndexInfo getIndexInfo(ClusteringPrefix name, ClusteringComparator comparator, boolean reversed)
    {
        throw new UnsupportedOperationException();
    }

    public IndexInfo next()
    {
        throw new UnsupportedOperationException();
    }

    public boolean hasNext()
    {
        throw new UnsupportedOperationException();
    }

    public IndexInfo peek()
    {
        throw new UnsupportedOperationException();
    }

    public void startIteratorAt(ClusteringPrefix name, ClusteringComparator comparator, boolean reversed)
    {
        throw new UnsupportedOperationException();
    }

    public void reset(boolean reversed)
    {
        throw new UnsupportedOperationException();
    }

    public boolean isReversed() {
        throw new UnsupportedOperationException();
    }

    public void close()
    {if (reader != null)
        FileUtils.closeQuietly(reader);
    }
}
