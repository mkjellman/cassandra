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
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.birch.IndexInfoSerializer;
import org.apache.cassandra.io.sstable.birch.PageAlignedWriter;
import org.apache.cassandra.io.sstable.birch.TreeSerializable;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * Created by mkjellman on 1/12/17.
 */
public class IndexInfo implements TreeSerializable
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new IndexInfo(null, null, 0, 0, null));

    public static final IndexInfoSerializer SERIALIZER = new IndexInfoSerializer();

    private final long offset;
    private final long width;
    private final ClusteringPrefix firstName;
    private final ClusteringPrefix lastName;

    // If at the end of the index block there is an open range tombstone marker, this marker
    // deletion infos. null otherwise.
    //public final DeletionTime endOpenMarker;
    public DeletionTime endOpenMarker;

    public IndexInfo(ClusteringPrefix firstName,
                     ClusteringPrefix lastName,
                     long offset,
                     long width,
                     DeletionTime endOpenMarker)
    {
        this.firstName = firstName;
        this.lastName = lastName;
        this.offset = offset;
        this.width = width;
        this.endOpenMarker = endOpenMarker;
    }

    public void setEndOpenMarker(DeletionTime endOpenMarker)
    {
        this.endOpenMarker = endOpenMarker;
    }
    
    public static class Serializer
    {
        // This is the default index size that we use to delta-encode width when serializing so we get better vint-encoding.
        // This is imperfect as user can change the index size and ideally we would save the index size used with each index file
        // to use as base. However, that's a bit more involved a change that we want for now and very seldom do use change the index
        // size so using the default is almost surely better than using no base at all.
        public static final long WIDTH_BASE = 64 * 1024;

        private final int version;
        private final List<AbstractType<?>> clusteringTypes;

        public Serializer(Version version, List<AbstractType<?>> clusteringTypes)
        {
            this.version = version.correspondingMessagingVersion();
            this.clusteringTypes = clusteringTypes;
        }

        public void serialize(IndexInfo info, DataOutputPlus out) throws IOException
        {
            ClusteringPrefix.serializer.serialize(info.getFirstName(), out, version, clusteringTypes);
            ClusteringPrefix.serializer.serialize(info.getLastName(), out, version, clusteringTypes);
            out.writeUnsignedVInt(info.offset);
            out.writeVInt(info.width - WIDTH_BASE);

            out.writeBoolean(info.endOpenMarker != null);
            if (info.endOpenMarker != null)
                DeletionTime.serializer.serialize(info.endOpenMarker, out);
        }

        public IndexInfo deserialize(DataInputPlus in) throws IOException
        {
            ClusteringPrefix firstName = ClusteringPrefix.serializer.deserialize(in, version, clusteringTypes);
            ClusteringPrefix lastName = ClusteringPrefix.serializer.deserialize(in, version, clusteringTypes);
            long offset = in.readUnsignedVInt();
            long width = in.readVInt() + WIDTH_BASE;
            DeletionTime endOpenMarker = null;
            if (in.readBoolean())
                endOpenMarker = DeletionTime.serializer.deserialize(in);
            return new IndexInfo(firstName, lastName, offset, width, endOpenMarker);
        }

        public long serializedSize(IndexInfo info)
        {
            long size = ClusteringPrefix.serializer.serializedSize(info.firstName, version, clusteringTypes)
                        + ClusteringPrefix.serializer.serializedSize(info.lastName, version, clusteringTypes)
                        + TypeSizes.sizeofUnsignedVInt(info.offset)
                        + TypeSizes.sizeofVInt(info.width - WIDTH_BASE)
                        + TypeSizes.sizeof(info.endOpenMarker != null);

            if (info.endOpenMarker != null)
                size += DeletionTime.serializer.serializedSize(info.endOpenMarker);
            return size;
        }
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE
               + firstName.unsharedHeapSize()
               + lastName.unsharedHeapSize()
               + (endOpenMarker == null ? 0 : endOpenMarker.unsharedHeapSize());
    }

    public ClusteringPrefix getFirstName()
    {
        return firstName;
    }

    public ClusteringPrefix getLastName()
    {
        return lastName;
    }

    public long getOffset()
    {
        return offset;
    }

    public long getWidth()
    {
        return width;
    }


    public ByteBuffer serializedKey(ClusteringComparator comparator) throws IOException
    {
        return ClusteringPrefix.serializer.serialize(getFirstName(), 0, comparator.subtypes());
    }

    public void serializeValue(PageAlignedWriter writer) throws IOException
    {
        SERIALIZER.serializeValue(this, writer);
    }

    public int serializedKeySize(ClusteringComparator comparator) throws IOException
    {
        ByteBuffer key = serializedKey(comparator);
        return key.limit() - key.position();
    }

    public int serializedValueSize()
    {
        return Long.BYTES + Long.BYTES + Byte.BYTES;
    }
}
