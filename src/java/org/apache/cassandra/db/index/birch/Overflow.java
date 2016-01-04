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

import java.nio.ByteBuffer;

/**
 * Used while creating a new {@link org.apache.cassandra.db.index.birch.BirchWriter}
 * tree to keep keys added to he tree that cannot fit within a single Birch
 * {@link org.apache.cassandra.db.index.birch.Node}.
 *
 * The overflow page is now internally padded and chunked along page boundaries,
 * but is still padded to the nearest page boundary.
 * <p>
 * It's assumed that the Overflow segment (if used) will never contain
 * more than 2GB.
 * <p>
 * While reading a BirchWriter tree {@link org.apache.cassandra.db.index.birch.BirchReader},
 * we will map the overflow page (if the tree has one) as a single mmapp'ed buffer.
 *
 * @see BirchWriter
 */
public class Overflow
{
    // do not allocate buffers by default as the "normal" case should be no overflow page
    private ByteBuffer overflowBuf = null;
    private byte[] buffer = null;

    public Overflow()
    {
    }

    public int add(ByteBuffer elm)
    {
        if (overflowBuf == null)
            overflowBuf = ByteBuffer.allocate(elm.remaining() * 2);

        int startPos = overflowBuf.position();

        if (overflowBuf.remaining() <= elm.remaining() + Integer.BYTES)
        {
            int newCapacity = (overflowBuf.capacity() * 3) / 2 + 1;

            // check if target size for resized buffer will fit new element
            if (newCapacity < overflowBuf.position() + (startPos + (elm.remaining() + Integer.BYTES)))
            {
                newCapacity += elm.remaining() + Integer.BYTES;
            }
            ByteBuffer resizedBuf = ByteBuffer.allocate(newCapacity);
            overflowBuf.position(0);
            resizedBuf.put(overflowBuf);
            overflowBuf = resizedBuf;
            overflowBuf.position(startPos);
        }

        if (buffer == null)
            buffer = new byte[1 << 12];

        overflowBuf.putInt(elm.remaining());

        while (elm.hasRemaining())
        {
            int toTransfer = (elm.remaining() > buffer.length) ? buffer.length : elm.remaining();
            elm.get(buffer, 0, toTransfer);
            overflowBuf.put(buffer, 0, toTransfer);
        }

        return startPos;
    }

    public ByteBuffer getOverflowBuf()
    {
        return overflowBuf;
    }
}
