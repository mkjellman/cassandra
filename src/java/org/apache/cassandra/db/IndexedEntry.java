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
import org.apache.cassandra.io.sstable.birch.BirchWriter;
import org.apache.cassandra.io.util.PageAlignedReader;
import org.apache.cassandra.io.sstable.birch.PageAlignedWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.schema.TableMetadata;

public interface IndexedEntry extends IMeasurableMemory, Iterator<IndexInfo>, AutoCloseable
{
    IndexInfo getIndexInfo(ClusteringPrefix name, boolean reversed) throws IOException;

    void setIteratorBounds(ClusteringBound start, ClusteringBound end, boolean reversed) throws IOException;

    /**
     * @return the offset to the start of the header information for this row.
     * For some formats this may not be the start of the row.
     */
    //long headerOffset();

    /**
     * The length of the row header (partition key, partition deletion and static row).
     * This value is only provided for indexed entries and this method will throw
     * {@code UnsupportedOperationException} if {@code !isIndexed()}.
     */
    long headerLength();

    void reset(boolean reversed);

    void close();

    boolean isReversed();

    void setReversed(boolean reversed);

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
        private final TableMetadata metadata;
        private final IndexInfo.Serializer idxSerializer;
        private final Version version;
        private final SerializationHeader header;

        public Serializer(TableMetadata metadata, Version version, SerializationHeader header)
        {
            this.metadata = metadata;
            this.idxSerializer = new IndexInfo.Serializer(version, header.clusteringTypes());
            this.version = version;
            this.header = header;
        }

        public void serialize(IndexedEntry rie, PageAlignedWriter writer) throws IOException
        {
            writer.writeBoolean(rie.isIndexed()); // is following serializated index entry indexed?
            writer.writeLong(rie.getPosition());

            if (rie.isIndexed())
            {
                writer.writeLong(rie.headerLength());
                DeletionTime.serializer.serialize(rie.deletionTime(), writer.stream);
                writer.finalizeCurrentSubSegment();
                BirchWriter birchWriter = new BirchWriter.Builder(rie.getAllColumnIndexes().iterator(), BirchWriter.SerializerType.INDEXINFO, metadata.comparator).build();
                writer.startNewSubSegment(birchWriter.getCacheLineSize());
                birchWriter.serialize(writer);
                writer.finalizeCurrentSubSegment();
                writer.finalizeCurrentSegment();
            }
            else
            {
                writer.finalizeCurrentSubSegment();
                writer.finalizeCurrentSegment();
            }
        }

        public IndexedEntry deserialize(DataInputPlus in) throws IOException
        {
            // instanceof check is a "hack" for now to use the "old"/legacy/pre-birch deserialization
            // logic with the AutoSavingCache
            if (version.hasBirchIndexes())
            {
                // todo kjkj: should we do a copy here?
                PageAlignedReader reader = (PageAlignedReader) in;

                boolean isIndexed = reader.readBoolean();
                //long position = in.readUnsignedVInt();
                long position = reader.readLong();

                if (isIndexed)
                {
                    long headerLength = reader.readLong();
                    //long headerLength = reader.readUnsignedVInt();
                    DeletionTime deletionTime = DeletionTime.serializer.deserialize(reader);
                    reader.nextSubSegment();
                    //PageAlignedReader readerCopy = PageAlignedReader.copy((PageAlignedReader) in);
                    IndexedEntry entry = new BirchIndexedEntry(position, metadata, header, version,
                                                               reader, headerLength, deletionTime);
                    reader.seekToEndOfCurrentSubSegment();
                    return entry;
                }
                else
                {
                    return new NonIndexedRowEntry(position, 0, reader);
                }
            }
            else
            {
                long position = in.readUnsignedVInt();

                int size = (int) in.readUnsignedVInt();
                if (size > 0)
                {
                    long headerLength = in.readUnsignedVInt();
                    DeletionTime deletionTime = DeletionTime.serializer.deserialize(in);
                    int entries = (int) in.readUnsignedVInt();
                    List<IndexInfo> columnsIndex = new ArrayList<>(entries);
                    for (int i = 0; i < entries; i++)
                        columnsIndex.add(idxSerializer.deserialize(in));

                    in.skipBytesFully(entries * TypeSizes.sizeof(0));

                    return new OnHeapIndexedEntry(position, deletionTime, headerLength, columnsIndex, null, metadata);
                }
                else
                {
                    // todo kjkj: does the old format expect a headerlength to be serialized??
                    return new NonIndexedRowEntry(position, 0, null);
                }
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
            if (version.hasBirchIndexes())
            {
                PageAlignedReader reader = (PageAlignedReader) in;

                // todo: kjkj can this be cleaned up/simplified? e.g. i think the logic should/could be,
                // always set to start of next segment unless current segment is the last segment, in
                // which case seek to the end of the current segment
                if (reader.getCurrentSegmentIdx() == reader.numberOfSegments() - 1)
                {
                    // if the current sub-segment is the last available sub-segment (and current is the last segment)
                    // seek to the end of the sub-segment to ensure isEOF will return true regardless of iteration order
                    if (reader.getCurrentSubSegmentIdx() == reader.numberOfSubSegments() - 1)
                    {
                        reader.seekToEndOfCurrentSegment();
                    }
                    else
                    {
                        // skip to the beginning of the next sub-segment
                        reader.setSegment(reader.getCurrentSegmentIdx(), reader.getCurrentSubSegmentIdx() + 1);
                    }
                }
                else
                {
                    reader.setSegment(reader.getCurrentSegmentIdx() + 1);
                }
            }
            else
            {
                readPosition(in, version);
                skipPromotedIndex(in);
            }
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

                indexedSize += TypeSizes.sizeof(rie.headerLength());
                //indexedSize += TypeSizes.sizeofUnsignedVInt(rie.headerLength());
                indexedSize += DeletionTime.serializer.serializedSize(rie.deletionTime());
                //indexedSize += TypeSizes.sizeofUnsignedVInt(index.size());
                indexedSize += TypeSizes.sizeof(index.size());

                for (IndexInfo info : index)
                    indexedSize += idxSerializer.serializedSize(info);

                indexedSize += index.size() * TypeSizes.sizeof(0);
            }

            return TypeSizes.sizeof(rie.getPosition()) + TypeSizes.sizeof(indexedSize) + indexedSize;
            //return TypeSizes.sizeofUnsignedVInt(rie.getPosition()) + TypeSizes.sizeofUnsignedVInt(indexedSize) + indexedSize;
        }
    }
}
