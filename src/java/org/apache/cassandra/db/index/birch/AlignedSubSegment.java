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

package org.apache.cassandra.db.index.birch;

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

public class AlignedSubSegment
{
    public static final AlignedSubSegmentSerializer SERIALIZER = new AlignedSubSegmentSerializer();

    public final int idx;
    public final long offset;
    public final long length;
    public final long alignedLength;
    public final int pageChunkSize;

    public AlignedSubSegment(int idx, long offset, long length, long alignedLength, int pageChunkSize)
    {
        this.idx = idx;
        this.offset = offset;
        this.length = length;
        this.alignedLength = alignedLength;
        this.pageChunkSize = pageChunkSize;
    }

    public boolean shouldUseSingleMmappedBuffer()
    {
        return pageChunkSize <= 0;
    }

    public long getEndOffset()
    {
        return offset + length;
    }

    public long getAlignedEndOffset()
    {
        return offset + alignedLength;
    }

    public boolean isPositionInSubSegment(long pos)
    {
        return pos >= offset && pos <= getEndOffset();
    }

    @Override
    public String toString()
    {
        return String.format("idx: %d offset: %d length: %d alignedLength: %d pageChunkSize: %d",
                             idx, offset, length, alignedLength, pageChunkSize);
    }

    public static class AlignedSubSegmentSerializer implements ISerializer<AlignedSubSegment>
    {

        public void serialize(AlignedSubSegment subSegment, DataOutputPlus out) throws IOException
        {
            out.writeInt(subSegment.idx);
            out.writeLong(subSegment.offset);
            out.writeLong(subSegment.length);
            out.writeLong(subSegment.alignedLength);
            out.writeInt(subSegment.pageChunkSize);
        }

        public AlignedSubSegment deserialize(DataInput in) throws IOException
        {
            int idx = in.readInt();
            long offset = in.readLong();
            long length = in.readLong();
            long alignedLength = in.readLong();
            int pageChunkSize = in.readInt();
            return new AlignedSubSegment(idx, offset, length, alignedLength, pageChunkSize);
        }

        public long serializedSize(AlignedSubSegment subSegment, TypeSizes type)
        {
            long size = 0;

            size += type.sizeof(subSegment.idx);
            size += type.sizeof(subSegment.offset);
            size += type.sizeof(subSegment.length);
            size += type.sizeof(subSegment.alignedLength);
            size += type.sizeof(subSegment.pageChunkSize);

            return size;
        }
    }
}
