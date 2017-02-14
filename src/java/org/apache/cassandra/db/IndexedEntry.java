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
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Created by mkjellman on 1/12/17.
 */
public interface IndexedEntry extends IMeasurableMemory, Iterator<IndexInfo>
{
    IndexInfo getIndexInfo(ClusteringPrefix name, ClusteringComparator comparator, boolean reversed) throws IOException;

    void startIteratorAt(ClusteringPrefix name, ClusteringComparator comparator, boolean reversed) throws IOException;

    /**
     * The length of the row header (partition key, partition deletion and static row).
     * This value is only provided for indexed entries and this method will throw
     * {@code UnsupportedOperationException} if {@code !isIndexed()}.
     */
    long headerLength(); // kjkj remove this is stupid just using to confirm refactor

    void reset(boolean reversed);

    void close();

    boolean isReversed();

    IndexInfo peek();

    long getPosition();

    /**
     * @return true if this index entry contains the row-level tombstone and column summary.  Otherwise,
     * caller should fetch these from the row header.
     */
    boolean isIndexed();

    int entryCount();

    int promotedSize(IndexInfo.Serializer type);

    DeletionTime deletionTime();

    List<IndexInfo> getAllColumnIndexes();

    public static class Serializer
    {
        private final IndexInfo.Serializer idxSerializer;
        private final Version version;

        public Serializer(Version version, SerializationHeader header)
        {
            this.idxSerializer = new IndexInfo.Serializer(version, header.clusteringTypes());
            this.version = version;
        }

        public void serialize(IndexedEntry rie, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt(rie.getPosition());
            out.writeUnsignedVInt(rie.promotedSize(idxSerializer));

            if (rie.isIndexed())
            {
                out.writeUnsignedVInt(rie.headerLength());
                DeletionTime.serializer.serialize(rie.deletionTime(), out);
                out.writeUnsignedVInt(rie.entryCount());

                // Calculate and write the offsets to the IndexInfo objects.

                int[] offsets = new int[rie.entryCount()];

                if (out.hasPosition())
                {
                    // Out is usually a SequentialWriter, so using the file-pointer is fine to generate the offsets.
                    // A DataOutputBuffer also works.
                    long start = out.position();
                    int i = 0;
                    for (IndexInfo info : rie.getAllColumnIndexes())
                    {
                        offsets[i] = i == 0 ? 0 : (int)(out.position() - start);
                        i++;
                        idxSerializer.serialize(info, out);
                    }
                }
                else
                {
                    // Not sure this branch will ever be needed, but if it is called, it has to calculate the
                    // serialized sizes instead of simply using the file-pointer.
                    int i = 0;
                    int offset = 0;
                    for (IndexInfo info : rie.getAllColumnIndexes())
                    {
                        offsets[i++] = offset;
                        idxSerializer.serialize(info, out);
                        offset += idxSerializer.serializedSize(info);
                    }
                }

                for (int off : offsets)
                    out.writeInt(off);
            }
        }

        public IndexedEntry deserialize(DataInputPlus in) throws IOException
        {
            long position = in.readUnsignedVInt();

            int size = (int)in.readUnsignedVInt();
            if (size > 0)
            {
                long headerLength = in.readUnsignedVInt();
                DeletionTime deletionTime = DeletionTime.serializer.deserialize(in);
                int entries = (int)in.readUnsignedVInt();
                List<IndexInfo> columnsIndex = new ArrayList<>(entries);
                for (int i = 0; i < entries; i++)
                    columnsIndex.add(idxSerializer.deserialize(in));

                in.skipBytesFully(entries * TypeSizes.sizeof(0));

                return new OnHeapIndexedEntry(position, deletionTime, headerLength, columnsIndex, null);
            }
            else
            {
                return new NonIndexedRowEntry(position, null);
            }
        }

        // Reads only the data 'position' of the index entry and returns it. Note that this left 'in' in the middle
        // of reading an entry, so this is only useful if you know what you are doing and in most case 'deserialize'
        // should be used instead.
        public static long readPosition(DataInputPlus in, Version version) throws IOException
        {
            return in.readUnsignedVInt();
        }

        public static void skip(DataInputPlus in, Version version) throws IOException
        {
            readPosition(in, version);
            skipPromotedIndex(in);
        }

        private static void skipPromotedIndex(DataInputPlus in) throws IOException
        {
            int size = (int)in.readUnsignedVInt();
            if (size <= 0)
                return;

            in.skipBytesFully(size);
        }

        public int serializedSize(IndexedEntry rie)
        {
            int indexedSize = 0;
            if (rie.isIndexed())
            {
                List<IndexInfo> index = rie.getAllColumnIndexes();

                indexedSize += TypeSizes.sizeofUnsignedVInt(rie.headerLength());
                indexedSize += DeletionTime.serializer.serializedSize(rie.deletionTime());
                indexedSize += TypeSizes.sizeofUnsignedVInt(index.size());

                for (IndexInfo info : index)
                    indexedSize += idxSerializer.serializedSize(info);

                indexedSize += index.size() * TypeSizes.sizeof(0);
            }

            return TypeSizes.sizeofUnsignedVInt(rie.getPosition()) + TypeSizes.sizeofUnsignedVInt(indexedSize) + indexedSize;
        }
    }
}
