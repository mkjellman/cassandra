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
package org.apache.cassandra.transport.messages;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.EnumUtils;

import io.netty.buffer.ByteBuf;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.transport.frame.ChecksummingFrameCompressor;
import org.apache.cassandra.transport.frame.LegacyLZ4FrameCompressor;
import org.apache.cassandra.transport.frame.LegacySnappyFrameCompressor;
import org.apache.cassandra.transport.frame.checksum.ChecksummingTransformer;
import org.apache.cassandra.transport.frame.compress.LZ4Compressor;
import org.apache.cassandra.transport.frame.compress.NoOpCompressor;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.ChecksumType;

/**
 * The initial message of the protocol.
 * Sets up a number of connection options.
 */
public class StartupMessage extends Message.Request
{
    public static final String CQL_VERSION = "CQL_VERSION";
    public static final String COMPRESSION = "COMPRESSION";
    public static final String PROTOCOL_VERSIONS = "PROTOCOL_VERSIONS";
    public static final String CHECKSUM = "CHECKSUM";

    public static final Message.Codec<StartupMessage> codec = new Message.Codec<StartupMessage>()
    {
        public StartupMessage decode(ByteBuf body, ProtocolVersion version)
        {
            return new StartupMessage(upperCaseKeys(CBUtil.readStringMap(body)));
        }

        public void encode(StartupMessage msg, ByteBuf dest, ProtocolVersion version)
        {
            CBUtil.writeStringMap(msg.options, dest);
        }

        public int encodedSize(StartupMessage msg, ProtocolVersion version)
        {
            return CBUtil.sizeOfStringMap(msg.options);
        }
    };

    public final Map<String, String> options;

    public StartupMessage(Map<String, String> options)
    {
        super(Message.Type.STARTUP);
        this.options = options;
    }

    public Message.Response execute(QueryState state, long queryStartNanoTime)
    {
        String cqlVersion = options.get(CQL_VERSION);
        if (cqlVersion == null)
            throw new ProtocolException("Missing value CQL_VERSION in STARTUP message");

        try
        {
            if (new CassandraVersion(cqlVersion).compareTo(new CassandraVersion("2.99.0")) < 0)
                throw new ProtocolException(String.format("CQL version %s is not supported by the binary protocol (supported version are >= 3.0.0)", cqlVersion));
        }
        catch (IllegalArgumentException e)
        {
            throw new ProtocolException(e.getMessage());
        }

        boolean useCompression = options.containsKey(COMPRESSION);
        boolean supportsChecksums = connection.getVersion().supportsChecksums();

        Optional<ChecksumType> checksumType = (supportsChecksums) ? getChecksumType() : Optional.empty();

        if (useCompression)
        {
            String compression = options.get(COMPRESSION).toLowerCase();
            switch (compression)
            {
                case "snappy":
                {
                    // no reason
                    if (!supportsChecksums)
                    {
                        if (LegacySnappyFrameCompressor.instance == null)
                            throw new ProtocolException("This instance does not support Snappy compression");
                        connection.setCompressor(LegacySnappyFrameCompressor.instance);
                        break;
                    }
                    else
                    {
                        throw new ProtocolException("Snappy compression is no longer supported. " +
                                                    "Please switch to the LZ4 compressor or another " +
                                                    "supported compression implementation.");
                    }
                }
                case "lz4":
                {
                    if (supportsChecksums && checksumType.isPresent())
                    {
                        ChecksummingFrameCompressor lz4ChecksummingFrameCompressor = new ChecksummingFrameCompressor(new ChecksummingTransformer(ChecksummingTransformer.DEFAULT_BLOCK_SIZE,
                                                                                                                                                 new LZ4Compressor(),
                                                                                                                                                 checksumType.get()));
                        connection.setCompressor(lz4ChecksummingFrameCompressor);
                    }
                    else
                    {
                        connection.setCompressor(LegacyLZ4FrameCompressor.instance);
                    }
                    break;
                }
                default: throw new ProtocolException(String.format("Unknown compression algorithm: %s", compression));
            }
        }
        else
        {
            if (supportsChecksums && checksumType.isPresent())
            {
                ChecksummingFrameCompressor checksumOnlyFrameCompressor = new ChecksummingFrameCompressor(new ChecksummingTransformer(ChecksummingTransformer.DEFAULT_BLOCK_SIZE,
                                                                                                                                      new NoOpCompressor(),
                                                                                                                                      checksumType.get()));
                connection.setCompressor(checksumOnlyFrameCompressor);
            }

            if (checksumType.isPresent() && !DatabaseDescriptor.isNativeTransportChecksummingEnabled())
            {
                throw new ProtocolException("This instance does not support checksummed native transport requests. " +
                                            "Either enable checksumming by setting YAML property " +
                                            "enable_checksumming_in_native_transport to true or disable checksums " +
                                            "on the connection in your client");
            }
        }

        if (DatabaseDescriptor.getAuthenticator().requireAuthentication())
            return new AuthenticateMessage(DatabaseDescriptor.getAuthenticator().getClass().getName());
        else
            return new ReadyMessage();
    }

    private static Map<String, String> upperCaseKeys(Map<String, String> options)
    {
        Map<String, String> newMap = new HashMap<String, String>(options.size());
        for (Map.Entry<String, String> entry : options.entrySet())
            newMap.put(entry.getKey().toUpperCase(), entry.getValue());
        return newMap;
    }

    @Override
    public String toString()
    {
        return "STARTUP " + options;
    }

    private Optional<ChecksumType> getChecksumType() throws ProtocolException
    {
        Optional<ChecksumType> checksumType;
        if (options.containsKey(CHECKSUM))
        {
            checksumType = Optional.ofNullable(EnumUtils.getEnum(ChecksumType.class, options.get(CHECKSUM)));
            if (!checksumType.isPresent())
            {
                throw new ProtocolException(String.format("Requsted checksum type %s is not known or supported by " +
                                                          "this version of Cassandra", options.get(CHECKSUM)));
            }
        }
        else
        {
            checksumType = Optional.empty();
        }
        return checksumType;
    }
}
