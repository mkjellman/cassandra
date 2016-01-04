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
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

public class AlignedSegment
{
    public static final AlignedSegmentSerializer SERIALIZER = new AlignedSegmentSerializer();

    public final int idx;
    public final long offset;
    public final long endOffset;
    public final long alignedEndOffset;
    public final long length;
    public final long alignedLength;
    private final List<AlignedSubSegment> subSegments;

    public AlignedSegment(int idx, long offset, long length, long alignedLength)
    {
        this(idx, offset, length, alignedLength, new ArrayList<AlignedSubSegment>());
    }

    public AlignedSegment(int idx, long offset, long length, long alignedLength, List<AlignedSubSegment> subSegments)
    {
        this.idx = idx;
        this.offset = offset;
        this.length = length;
        this.alignedLength = alignedLength;
        this.endOffset = offset + length;
        this.alignedEndOffset = offset + alignedLength;
        this.subSegments = subSegments;
    }

    public void addSubSegment(AlignedSubSegment subSegment)
    {
        subSegments.add(subSegment);
    }

    public List<AlignedSubSegment> getSubSegments()
    {
        return subSegments;
    }

    public AlignedSubSegment getLastSubSegment()
    {
        return subSegments.get(subSegments.size() - 1);
    }

    public boolean isPositionInSegment(long pos)
    {
        return pos >= offset && pos <= endOffset;
    }

    @Override
    public String toString()
    {
        Joiner joiner = Joiner.on(" | ");
        String joinedSubSegments = joiner.join(subSegments);
        return String.format("[Segment %d] offset: %d length: %d alignedLength: %d " +
                             "numSubSegments: %d {%s}", idx, offset, length, alignedLength,
                             subSegments.size(), joinedSubSegments);
    }

    public static class AlignedSegmentSerializer implements ISerializer<AlignedSegment>
    {
        public void serialize(AlignedSegment alignedSegment, DataOutputPlus out) throws IOException
        {
            out.writeInt(alignedSegment.idx);
            out.writeLong(alignedSegment.offset);
            out.writeLong(alignedSegment.length);
            out.writeLong(alignedSegment.alignedLength);

            out.writeInt(alignedSegment.subSegments.size());

            for (AlignedSubSegment subSegment : alignedSegment.subSegments)
            {
                AlignedSubSegment.SERIALIZER.serialize(subSegment, out);
            }
        }

        public AlignedSegment deserialize(DataInput in) throws IOException
        {
            int idx = in.readInt();
            long offset = in.readLong();
            long length = in.readLong();
            long alignedLength = in.readLong();

            AlignedSegment segment = new AlignedSegment(idx, offset, length, alignedLength);

            int numSubSegments = in.readInt();

            for (int i = 0; i < numSubSegments; i++)
            {
                segment.addSubSegment(AlignedSubSegment.SERIALIZER.deserialize(in));
            }

            return segment;
        }

        public long serializedSize(AlignedSegment alignedSegment, TypeSizes type)
        {
            long size = 0;

            size += type.sizeof(alignedSegment.idx);
            size += type.sizeof(alignedSegment.offset);
            size += type.sizeof(alignedSegment.length);
            size += type.sizeof(alignedSegment.alignedLength);

            size += type.sizeof(alignedSegment.subSegments.size());

            for (AlignedSubSegment subSegment : alignedSegment.subSegments)
            {
                size += AlignedSubSegment.SERIALIZER.serializedSize(subSegment, type);
            }

            return size;
        }
    }
}
