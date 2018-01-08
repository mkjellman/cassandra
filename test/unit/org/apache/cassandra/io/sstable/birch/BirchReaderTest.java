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

package org.apache.cassandra.io.sstable.birch;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.IndexInfo;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.PageAlignedReader;
import org.apache.cassandra.schema.KeyspaceParams;

/**
 * Tests the correctness of BirchReader. Many related/additional tests
 * can be found in PageAlignedReaderTest, PageAlignedWriterTest,
 * and BirchWriterTest.
 */
public class BirchReaderTest extends SchemaLoader
{
    protected static final String KEYSPACE = "BirchReaderTest";
    protected static final String CF = "StandardTimeUUID1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();

        /*
        if (FBUtilities.isWindows)
        {
            standardMode = DatabaseDescriptor.getDiskAccessMode();
            indexMode = DatabaseDescriptor.getIndexAccessMode();

            DatabaseDescriptor.setDiskAccessMode(Config.DiskAccessMode.standard);
            DatabaseDescriptor.setIndexAccessMode(Config.DiskAccessMode.standard);
        }
        */

        //ksName, cfName, columnCount, keyType, valType, AsciiType.instance
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF, 1, TimeUUIDType.instance, TimeUUIDType.instance));

        //maxValueSize = DatabaseDescriptor.getMaxValueSize();
        DatabaseDescriptor.setMaxValueSize(1024 * 1024); // set max value size to 1MB
    }

    @Test
    public void testForwardsIteration() throws Exception
    {
        testIteration(3000, false);
    }

    @Test
    public void testReversedIteration() throws Exception
    {
        testIteration(3000, true);
    }

    private void testIteration(int targetTreeSize, boolean reversed) throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), ".birch");

        PageAlignedWriter writer = new PageAlignedWriter(tmpFile);

        Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(targetTreeSize, 100);
        BirchWriter birchWriter = new BirchWriter.Builder(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, cfs.getComparator()).build();

        writer.startNewSegment();
        writer.startNewSubSegment(birchWriter.getCacheLineSize());
        birchWriter.serialize(writer);
        writer.finalizeCurrentSubSegment();
        writer.finalizeCurrentSegment();
        writer.prepareToCommit();
        writer.close();

        PageAlignedReader reader = PageAlignedReader.open(tmpFile);
        reader.setSegment(0);
        SerializationHeader serializationHeader = new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS);
        try (BirchReader birchReader = new BirchReader(reader, serializationHeader, BigFormat.instance.getVersion("na")))
        {
            // IndexInfo doesn't override equals, so put the bytes in the set
            // and check that for the samples in this case...
            Set<ByteBuffer> sampleNames = new HashSet<>();
            for (IndexInfo sample : ((TimeUUIDTreeSerializableIterator) timeUUIDIterator).getSamples())
            {
                sampleNames.add(sample.serializedKey(cfs.getComparator()));
            }

            BirchReader.BirchIterator iterator = birchReader.getIterator(cfs.metadata.get(), reversed);

            int iteratorCount = 0;
            int sampleMatches = 0;
            while (iterator.hasNext())
            {
                IndexInfo next = (IndexInfo) iterator.next();
                if (sampleNames.contains(next.serializedKey(cfs.getComparator())))
                    sampleMatches++;
                iteratorCount++;
            }

            Assert.assertEquals(birchReader.getElementCount(), iteratorCount);
            Assert.assertEquals(targetTreeSize, iteratorCount);
            Assert.assertEquals(sampleNames.size(), sampleMatches);
        }
    }
}
