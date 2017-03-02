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

package org.apache.cassandra.io.sstable.birch;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.IndexInfo;
import org.apache.cassandra.io.util.PageAlignedReader;


import static org.apache.cassandra.io.sstable.birch.BirchWriter.SerializerType;

/**
 * A Birch BSerializer implementation for {@link org.apache.cassandra.db.IndexInfo} objects.
 *
 * @see IndexInfo
 */
public class IndexInfoSerializer implements BSerializer<IndexInfo>
{
    public SerializerType getType()
    {
        return SerializerType.INDEXINFO;
    }

    public void serializeValue(IndexInfo obj, PageAlignedWriter writer) throws IOException
    {
        writer.writeLong(obj.getOffset());
        writer.writeLong(obj.getWidth());
        writer.writeBoolean(obj.endOpenMarker != null);
    }

    public IndexInfo deserializeValue(ByteBuffer key, ClusteringComparator type, PageAlignedReader reader) throws IOException
    {
        long offset = reader.readLong();
        long size = reader.readLong();
        boolean isDeleted = reader.readBoolean();
        ClusteringPrefix firstName = ClusteringPrefix.serializer.deserialize(key, 0, type.subtypes());
        DeletionTime deletionTime = (!isDeleted) ? null : new DeletionTime(1, 1);
        //DeletionTime deletionTime = (!isDeleted) ? null : new DeletionTime(Long.MIN_VALUE + 1, Integer.MAX_VALUE - 1);
        //DeletionTime deletionTime = null;
        return new IndexInfo(firstName, firstName, offset, size, deletionTime);
    }

    public int serializedValueSize()
    {
        return Long.BYTES + Long.BYTES + Byte.BYTES;
    }
}
