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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(Overflow.class);

    // do not allocate buffers by default as the "normal" case should be no overflow page
    private ByteBuffer overflowBuf = null;
    private byte[] buffer = null;

    public Overflow()
    {
    }

    public int add(ByteBuffer elm)
    {
        if (overflowBuf == null)
            overflowBuf = ByteBuffer.allocateDirect(elm.remaining() * 2);

        int startPos = overflowBuf.position();

        if (overflowBuf.remaining() <= elm.remaining() + Integer.BYTES)
        {
            // todo kj: need to deal with > 2GB Overflow.. for now just make sure we only do Integer.MAX_VALUE on resize
            // 2016-09-14 20:32:56,197 ERROR [MemtableFlushWriter:5] org.apache.cassandra.db.index.birch.Overflow -
            // Failed to allocate resized overflow buffer. newCapacity: -946725502 overflowBuf.capacity(): 799942638
            // overflowBuf.position(): 799218110 startPos: 799218110 elm.remaining(): 844184 elm.capacity(): 844184
            long idealCapacity = ((overflowBuf.capacity() * 3) / 2) + 1;
            int newCapacity = (idealCapacity > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) idealCapacity;

            // check if target size for resized buffer will fit new element
            if (newCapacity < overflowBuf.position() + (startPos + (elm.remaining() + Integer.BYTES)))
            {
                newCapacity += elm.remaining() + Integer.BYTES;
            }
            
            ByteBuffer resizedBuf;
            try
            {
                resizedBuf = ByteBuffer.allocateDirect(newCapacity);
            }
            catch (Exception e)
            {
                logger.error("Failed to allocate resized overflow buffer. newCapacity: {} overflowBuf.capacity(): {} " +
                             "overflowBuf.position(): {} startPos: {} elm.remaining(): {} elm.capacity(): {}", newCapacity,
                             overflowBuf.capacity(), overflowBuf.position(), startPos, elm.remaining(), elm.capacity());
                throw e;
            }

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
