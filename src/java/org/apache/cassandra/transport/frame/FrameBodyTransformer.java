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
import org.apache.cassandra.transport.frame.checksum.ChecksummingTransformer;

public interface FrameBodyTransformer
{
    /**
     * Compresses and consumes the entire length from the starting offset or reader index to the length
     * in a serialization format as described in {@link ChecksummingTransformer}
     * adding checksums and chunking to the frame body

     * @param inputBuf the input/source buffer of what we are going to compress
     * @return a single ByteBuf with all the compressed bytes serialized in the compressed/chunk'ed/checksum'ed format
     * @throws IOException if we fail while compressing the input blocks or read from the inputBuf itself
     */
    ByteBuf transformOutbound(ByteBuf inputBuf) throws IOException;

    /**
     * Decompresses the given inputBuf in one go, where inputBuf is serialized in the checksum'ed chunked format specified
     * @param inputBuf the entire compressed value serialized in the chunked and checksum'ed format described
     * @return the actual resulting decompressed bytes for usage (free of any serialization etc.)
     * @throws IOException if we failed to decompress or match a checksum check on a chunk
     */
    ByteBuf transformInbound(ByteBuf inputBuf) throws IOException;


}
