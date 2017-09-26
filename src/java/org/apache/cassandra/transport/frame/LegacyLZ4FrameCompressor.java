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

package org.apache.cassandra.transport.frame;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Frame;

/**
 * The legacy LZ4 Frame Compressor implementation. This compressor is
 * very simple -- encoding the length of the compressed bytes followed
 * by the output from the LZ4 compressor. This is the compressor
 * implementation that was used when a client would request LZ4 compression
 * but in protocol versions before checksuming was added.  When the decision
 * is made to drop support for clients that do not support checksums this
 * may (and should) be removed.
 */
public class LegacyLZ4FrameCompressor implements FrameCompressor
{
    public static final LegacyLZ4FrameCompressor instance = new LegacyLZ4FrameCompressor();

    private static final int INTEGER_BYTES = 4;
    private final LZ4Compressor compressor;
    private final LZ4FastDecompressor decompressor;

    private LegacyLZ4FrameCompressor()
    {
        final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
        compressor = lz4Factory.fastCompressor();
        decompressor = lz4Factory.fastDecompressor();
    }

    public Frame compress(Frame frame) throws IOException
    {
        byte[] input = CBUtil.readRawBytes(frame.body);

        int maxCompressedLength = compressor.maxCompressedLength(input.length);
        ByteBuf outputBuf = CBUtil.allocator.heapBuffer(INTEGER_BYTES + maxCompressedLength);

        byte[] output = outputBuf.array();
        int outputOffset = outputBuf.arrayOffset();

        output[outputOffset + 0] = (byte) (input.length >>> 24);
        output[outputOffset + 1] = (byte) (input.length >>> 16);
        output[outputOffset + 2] = (byte) (input.length >>>  8);
        output[outputOffset + 3] = (byte) (input.length);

        try
        {
            int written = compressor.compress(input, 0, input.length, output, outputOffset + INTEGER_BYTES, maxCompressedLength);
            outputBuf.writerIndex(INTEGER_BYTES + written);

            return frame.with(outputBuf);
        }
        catch (final Throwable e)
        {
            outputBuf.release();
            throw e;
        }
        finally
        {
            //release the old frame
            frame.release();
        }
    }

    public Frame decompress(Frame frame) throws IOException
    {
        byte[] input = CBUtil.readRawBytes(frame.body);

        int uncompressedLength = ((input[0] & 0xFF) << 24)
                                 | ((input[1] & 0xFF) << 16)
                                 | ((input[2] & 0xFF) <<  8)
                                 | ((input[3] & 0xFF));

        ByteBuf output = CBUtil.allocator.heapBuffer(uncompressedLength);

        try
        {
            int read = decompressor.decompress(input, INTEGER_BYTES, output.array(), output.arrayOffset(), uncompressedLength);
            if (read != input.length - INTEGER_BYTES)
                throw new IOException("Compressed lengths mismatch");

            output.writerIndex(uncompressedLength);

            return frame.with(output);
        }
        catch (final Throwable e)
        {
            output.release();
            throw e;
        }
        finally
        {
            //release the old frame
            frame.release();
        }
    }

    public boolean isChecksumming()
    {
        return false;
    }
}
