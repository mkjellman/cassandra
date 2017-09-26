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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyError;

/**
 * The legacy Snappy Frame Compressor implementation. This compressor is
 * very simple -- encoding the length of the compressed bytes followed
 * by the output from the Snappy compressor. All bytes are given to the
 * snappy library in one go -- and the resulting byte[] used in its
 * entirety. When the decision is made to drop support for clients that
 * do not support checksums this may (and should) be removed.
 * <p>
 * The use of Snappy is deprecated. Clients that support a version of
 * the native transport that support checksuming and later will fail.
 * LZ4 (and many other modern algorithms we could choose to implement)
 * are superior in almost every way to Snappy.
**/
public class LegacySnappyFrameCompressor implements FrameCompressor
{
    @Deprecated
    private static final Logger logger = LoggerFactory.getLogger(LegacySnappyFrameCompressor.class);

    public static final LegacySnappyFrameCompressor instance;
    static
    {
        LegacySnappyFrameCompressor i;
        try
        {
            i = new LegacySnappyFrameCompressor();
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            i = null;
        }
        catch (NoClassDefFoundError | SnappyError | UnsatisfiedLinkError e)
        {
            i = null;
        }
        instance = i;
    }

    private LegacySnappyFrameCompressor()
    {
        // this would throw java.lang.NoClassDefFoundError if Snappy class
        // wasn't found at runtime which should be processed by the calling method
        Snappy.getNativeLibraryVersion();

        logger.warn("You're using the deprecated Snappy compressor. Please switch to a modern supported alternative " +
                    "such as LZ4");
    }

    public Frame compress(Frame frame) throws IOException
    {
        byte[] input = CBUtil.readRawBytes(frame.body);
        ByteBuf output = CBUtil.allocator.heapBuffer(Snappy.maxCompressedLength(input.length));

        try
        {
            int written = Snappy.compress(input, 0, input.length, output.array(), output.arrayOffset());
            output.writerIndex(written);
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

        return frame.with(output);
    }

    public Frame decompress(Frame frame) throws IOException
    {
        byte[] input = CBUtil.readRawBytes(frame.body);

        if (!Snappy.isValidCompressedBuffer(input, 0, input.length))
            throw new ProtocolException("Provided frame does not appear to be Snappy compressed");

        ByteBuf output = CBUtil.allocator.heapBuffer(Snappy.uncompressedLength(input));

        try
        {
            int size = Snappy.uncompress(input, 0, input.length, output.array(), output.arrayOffset());
            output.writerIndex(size);
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

        return frame.with(output);
    }

    public boolean isChecksumming()
    {
        return false;
    }
}
