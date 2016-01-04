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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;

public class PageAlignedWriterTest
{
    private static final Random RANDOM = new Random();

    @Test
    public void simpleSingleSegment() throws Exception
    {
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");

        String randomLongWord = generateRandomWord((1 << 18) - 5);
        ByteBuffer randomLongWordBytes = ByteBuffer.wrap(randomLongWord.getBytes()).duplicate();

        try (PageAlignedWriter writer = new PageAlignedWriter(tmpFile))
        {
            writer.startNewSegment();
            writer.startNewNonPageAlignedSubSegment();
            writer.writeInt(randomLongWord.length());
            writer.write(randomLongWordBytes);
            writer.finalizeCurrentSubSegment();

            writer.startNewSubSegment(4096);
            for (int k = 0; k < 10; k++)
            {
                byte[] buf = new byte[4096];
                for (int i = 0; i < 4092; i++)
                {
                    buf[i] = 'a';
                }
                for (int i = 4092; i < 4096; i++)
                {
                    buf[i] = 'x';
                }
                writer.write(buf, 0, buf.length);
            }
            writer.finalizeCurrentSubSegment();
            writer.finalizeCurrentSegment();
        }

        try (PageAlignedReader reader = new PageAlignedReader(tmpFile))
        {
            reader.setSegment(0, 0);
            int encodedWordLength = reader.readInt();
            ByteBuffer encodedWord = reader.readBytes(encodedWordLength);
            randomLongWordBytes.position(0);
            Assert.assertTrue(UTF8Type.instance.compare(encodedWord, randomLongWordBytes) == 0);

            reader.setSegment(0, 1);
            char c = (char) reader.readByte();
            Assert.assertEquals('a', c);
            reader.seek(reader.getCurrentSubSegment().offset + 4);
            char c2 = (char) reader.readByte();
            Assert.assertEquals('a', c2);
            reader.seek(reader.getCurrentSubSegment().offset + (4096 * 2) + 24);
            char c3 = (char) reader.readByte();
            Assert.assertEquals('a', c3);
            reader.seek(reader.getCurrentSubSegment().offset + 4096 - 2);
            char c4 = (char) reader.readByte();
            Assert.assertEquals('x', c4);
        }

        tmpFile.delete();
    }

    @Test
    public void readFromCachePaddedSegment() throws Exception
    {
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");

        try (PageAlignedWriter writer = new PageAlignedWriter(tmpFile))
        {
            writer.startNewSegment();
            writer.startNewSubSegment(4096);

            for (int k = 0; k < 10; k++)
            {
                byte[] buf = new byte[4096];
                for (int i = 0; i < 4092; i++)
                {
                    buf[i] = 'a';
                }
                for (int i = 4092; i < 4096; i++)
                {
                    buf[i] = 'x';
                }
                writer.write(buf, 0, buf.length);
            }
            writer.finalizeCurrentSubSegment();
            writer.finalizeCurrentSegment();
        }

        try (PageAlignedReader reader = new PageAlignedReader(tmpFile))
        {
            reader.setSegment(0);

            reader.seek(4096);
            Assert.assertEquals(0, reader.getCurrentInternalBufferPosition());
            Assert.assertEquals(4096, reader.getFilePointer());
            reader.readByte();
            Assert.assertEquals(1, reader.getCurrentInternalBufferPosition());
            Assert.assertEquals(4096, reader.getFilePointer());
            reader.readLong();
            reader.readLong();
            reader.seek(4096);
            Assert.assertEquals(0, reader.getCurrentInternalBufferPosition());
            Assert.assertEquals(4096, reader.getFilePointer());
            reader.seek((4096 * 3) + 92);
            Assert.assertEquals((4096 * 3), reader.getFilePointer());
            Assert.assertEquals(92, reader.getCurrentInternalBufferPosition());
            reader.seek(4096 * 3);
            Assert.assertEquals((4096 * 3), reader.getFilePointer());
            Assert.assertEquals(0, reader.getCurrentInternalBufferPosition());
            reader.readByte();
            Assert.assertEquals((4096 * 3), reader.getFilePointer());
            Assert.assertEquals(1, reader.getCurrentInternalBufferPosition());
        }

        tmpFile.delete();
    }

    private static String generateRandomWord(int length)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length * 5; i++)
        {
            sb.append((char) (RANDOM.nextInt(26) + 'a'));
        }
        return sb.toString();
    }
}
