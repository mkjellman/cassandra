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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;
import java.util.UUID;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.IndexInfo;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.PageAlignedReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;

public class BirchWriterTest extends SchemaLoader
{
    private static final Logger logger = LoggerFactory.getLogger(BirchWriterTest.class);

    protected static final String KEYSPACE = "BirchWriterTest";
    protected static final String CF = "StandardTimeUUID1";

    protected static final Version CURRENT_VERSION = BigFormat.instance.getVersion("na");

    private static final Random RANDOM = new Random();

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
                                    SchemaLoader.standardCFMD(KEYSPACE, CF, 1, TimeUUIDType.instance, TimeUUIDType.instance, TimeUUIDType.instance));

        //maxValueSize = DatabaseDescriptor.getMaxValueSize();
        DatabaseDescriptor.setMaxValueSize(1024 * 1024); // set max value size to 1MB
    }

    @Test(timeout = 30000)
    public void wickedLargeTreeWithoutOverflow() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), ".birch");

        Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(100000, 5000);
        BirchWriter birchWriter = new BirchWriter.Builder(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, cfs.getComparator()).build();

        try (PageAlignedWriter writer = new PageAlignedWriter(tmpFile))
        {
            writer.startNewSegment();
            writer.startNewSubSegment(birchWriter.getCacheLineSize());
            birchWriter.serialize(writer);
            writer.finalizeCurrentSubSegment();
            writer.finalizeCurrentSegment();
        }

        Assert.assertTrue(tmpFile.delete());
    }

    @Test(timeout = 30000)
    public void multipleWickedLargeTreesWithoutOverflow() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), ".birch");

        try (PageAlignedWriter writer = new PageAlignedWriter(tmpFile))
        {
            for (int i = 0; i < 3; i++)
            {
                writer.startNewSegment();
                writer.startNewNonPageAlignedSubSegment();
                String keyStr = generateRandomWord(RANDOM.nextInt(50) + 3);
                ByteBuffer key = ByteBuffer.wrap(keyStr.getBytes()).duplicate();
                writer.writeShort(key.capacity());
                writer.write(key);
                writer.finalizeCurrentSubSegment();

                Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(50000 * (i + 1), 5000);
                List<IndexInfo> samples = ((TimeUUIDTreeSerializableIterator) timeUUIDIterator).getSamples();

                BirchWriter birchWriter = new BirchWriter.Builder(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, cfs.getComparator()).build();
                writer.startNewSubSegment(birchWriter.getCacheLineSize());
                birchWriter.serialize(writer);
                writer.finalizeCurrentSubSegment();
                writer.finalizeCurrentSegment();
            }
        }

        try (PageAlignedReader reader = PageAlignedReader.open(tmpFile))
        {
            reader.setSegment(0);
            Assert.assertTrue(reader.hasNextSubSegment());
            reader.setSegment(1);
            Assert.assertTrue(reader.hasNextSubSegment());
            reader.setSegment(2);
            Assert.assertTrue(reader.hasNextSubSegment());
        }

        Assert.assertTrue(tmpFile.delete());
    }

    @Test(timeout = 30000)
    public void wickedLargeTreeWithOverflow() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), ".birch");

        PageAlignedWriter writer = new PageAlignedWriter(tmpFile);

        Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(85000, 5000);
        List<IndexInfo> samples = ((TimeUUIDTreeSerializableIterator) timeUUIDIterator).getSamples();

        BirchWriter birchWriter = new BirchWriter.Builder(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, cfs.getComparator())
                                  .maxKeyLengthWithoutOverflow((short) 10) // set the max key length super small so we test overflow logic without needing large test input keys
                                  .build();

        writer.startNewSegment();
        writer.startNewSubSegment(birchWriter.getCacheLineSize());
        birchWriter.serialize(writer);
        writer.finalizeCurrentSubSegment();
        writer.finalizeCurrentSegment();
        writer.close();

        try (PageAlignedReader reader = PageAlignedReader.open(tmpFile))
        {
            reader.setSegment(0);
            SerializationHeader serializationHeader = new SerializationHeader(true, cfs.metadata(),
                                                                              cfs.metadata().regularAndStaticColumns(),
                                                                              EncodingStats.NO_STATS);
            try (BirchReader<IndexInfo> birchReader = new BirchReader<>(reader, serializationHeader, CURRENT_VERSION))
            {
                Assert.assertEquals(85000, birchReader.getElementCount());

                for (IndexInfo sample : samples)
                {
                    logger.info("looking for {}", sample.getFirstName().toString(cfs.metadata.get()));
                    IndexInfo indexInfo = birchReader.search(sample.getFirstName(), cfs.metadata.get(), false);
                    Assert.assertEquals(sample.getOffset(), indexInfo.getOffset());
                }
            }
        }

        Assert.assertTrue(tmpFile.delete());
    }

    @Test
    public void searchForExactMatch() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), ".birch");

        Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(10000, 300);
        List<IndexInfo> samples = ((TimeUUIDTreeSerializableIterator) timeUUIDIterator).getSamples();

        try (PageAlignedWriter writer = new PageAlignedWriter(tmpFile))
        {
            writer.startNewSegment();
            writer.startNewNonPageAlignedSubSegment();
            String keyStr = generateRandomWord(RANDOM.nextInt(50) + 3);
            ByteBuffer key = ByteBuffer.wrap(keyStr.getBytes()).duplicate();
            writer.writeShort(key.capacity());
            writer.write(key);
            writer.finalizeCurrentSubSegment();

            BirchWriter birchWriter = new BirchWriter.Builder(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, cfs.getComparator()).build();

            writer.startNewSubSegment(birchWriter.getCacheLineSize());
            birchWriter.serialize(writer);
            writer.finalizeCurrentSubSegment();
            writer.finalizeCurrentSegment();
        }

        try (PageAlignedReader reader = PageAlignedReader.open(tmpFile))
        {
            reader.setSegment(0);
            Assert.assertTrue(reader.hasNextSubSegment());
            reader.nextSubSegment();

            SerializationHeader serializationHeader = new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS);
            BirchReader<IndexInfo> birchReader = new BirchReader<>(reader, serializationHeader, CURRENT_VERSION);
            Assert.assertEquals(10000, birchReader.getElementCount());
            IndexInfo res = birchReader.search(samples.get(3).getFirstName(), cfs.metadata.get(), false);
            Assert.assertTrue(cfs.getComparator().compare(res.getFirstName(), samples.get(3).getFirstName()) == 0);
        }

        Assert.assertTrue(tmpFile.delete());
    }

    @Test
    public void multipleTreesAndIterateAllElementsForwards() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), ".birch");

        try (PageAlignedWriter writer = new PageAlignedWriter(tmpFile))
        {
            for (int i = 0; i < 3; i++)
            {
                writer.startNewSegment();
                writer.startNewNonPageAlignedSubSegment();
                String keyStr = generateRandomWord(RANDOM.nextInt(50) + 3);
                ByteBuffer key = ByteBuffer.wrap(keyStr.getBytes()).duplicate();
                writer.writeShort(key.capacity());
                writer.write(key);
                writer.finalizeCurrentSubSegment();

                Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(50000 * (i + 1), 300);
                BirchWriter birchWriter = new BirchWriter.Builder(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, cfs.getComparator()).build();

                writer.startNewSubSegment(birchWriter.getCacheLineSize());
                birchWriter.serialize(writer);
                writer.finalizeCurrentSubSegment();
                writer.finalizeCurrentSegment();
            }
        }

        try (PageAlignedReader reader = PageAlignedReader.open(tmpFile))
        {
            reader.setSegment(1);
            reader.nextSubSegment();

            SerializationHeader serializationHeader = new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS);
            BirchReader<IndexInfo> birchReader = new BirchReader<>(reader, serializationHeader, CURRENT_VERSION);
            Iterator<IndexInfo> iterator = birchReader.getIterator(cfs.metadata.get(), false);
            int iteratorCount = 0;
            while (iterator.hasNext())
            {
                IndexInfo next = iterator.next();
                iteratorCount++;
            }

            Assert.assertEquals(50000 * 2, iteratorCount);
        }

        Assert.assertTrue(tmpFile.delete());
    }

    @Test
    public void multipleTreesAndIterateAllElementsReversed() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), ".birch");

        int generatedIteratorBaseSize = 10000;

        List<IndexInfo> expected = null;
        try (PageAlignedWriter writer = new PageAlignedWriter(tmpFile))
        {
            for (int i = 0; i < 3; i++)
            {
                writer.startNewSegment();
                writer.startNewNonPageAlignedSubSegment();
                String keyStr = generateRandomWord(RANDOM.nextInt(50) + 3);
                ByteBuffer key = ByteBuffer.wrap(keyStr.getBytes()).duplicate();
                writer.writeShort(key.capacity());
                writer.write(key);
                writer.finalizeCurrentSubSegment();

                Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(generatedIteratorBaseSize * (i + 1), (i == 0) ? -1 : 0);
                if (i == 0)
                    expected = ((TimeUUIDTreeSerializableIterator) timeUUIDIterator).getSamples();

                BirchWriter birchWriter = new BirchWriter.Builder(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, cfs.getComparator()).build();

                writer.startNewSubSegment(birchWriter.getCacheLineSize());
                birchWriter.serialize(writer);
                writer.finalizeCurrentSubSegment();
                writer.finalizeCurrentSegment();
            }
        }

        try (PageAlignedReader reader = PageAlignedReader.open(tmpFile))
        {
            reader.setSegment(0);
            Assert.assertTrue(reader.hasNextSubSegment());
            reader.nextSubSegment();
            SerializationHeader serializationHeader = new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS);
            BirchReader birchReader = new BirchReader(reader, serializationHeader, CURRENT_VERSION);
            Assert.assertEquals(generatedIteratorBaseSize, expected.size());
            ListIterator<IndexInfo> expectedIterator = expected.listIterator(expected.size());
            Iterator<IndexInfo> iterator = birchReader.getIterator(cfs.metadata.get(), true);
            int iteratorCount = 0;
            while (iterator.hasNext())
            {
                IndexInfo next = iterator.next();
                IndexInfo expectedIndexInfo = expectedIterator.previous();

                logger.info("comparing next: {} to expected: {}", ByteBufferUtil.bytesToHex(next.serializedKey(cfs.getComparator())), ByteBufferUtil.bytesToHex(expectedIndexInfo.serializedKey(cfs.getComparator())));
                Assert.assertTrue(cfs.getComparator().compare(expectedIndexInfo.getFirstName(), next.getFirstName()) == 0);
                Assert.assertEquals(expectedIndexInfo.getOffset(), next.getOffset());
                Assert.assertEquals(expectedIndexInfo.getWidth(), next.getWidth());
                iteratorCount++;
            }

            Assert.assertEquals(10000, iteratorCount);
        }

        Assert.assertTrue(tmpFile.delete());
    }

    @Test(timeout = 1200000)
    public void absolutelyWickedMassiveTree() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), ".birch");

        Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(3000000, 1000000);

        try (PageAlignedWriter writer = new PageAlignedWriter(tmpFile))
        {
            BirchWriter birchWriter = new BirchWriter.Builder(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, cfs.getComparator()).build();
            writer.startNewSegment();
            writer.startNewSubSegment(birchWriter.getCacheLineSize());
            birchWriter.serialize(writer);
            writer.finalizeCurrentSubSegment();
            writer.finalizeCurrentSegment();
        }

        try (PageAlignedReader reader = PageAlignedReader.open(tmpFile))
        {
            reader.setSegment(0);

            SerializationHeader serializationHeader = new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS);
            BirchReader<IndexInfo> birchReader = new BirchReader<>(reader, serializationHeader, CURRENT_VERSION);

            List<IndexInfo> samples = ((TimeUUIDTreeSerializableIterator) timeUUIDIterator).getSamples();
            for (IndexInfo sample : samples)
            {
                IndexInfo indexInfo = birchReader.search(sample.getFirstName(), cfs.metadata.get(), false);
                /*
                logger.debug("birchWriter returned {}:{}:{}.... looking for {}:{}:{}",
                             TimeUUIDType.instance.getString(indexInfo.getFirstName()), indexInfo.getOffset(), indexInfo.getWidth(),
                             TimeUUIDType.instance.getString(sample.getFirstName()), sample.getOffset(), sample.getWidth());
                             */
                Assert.assertEquals(sample.getOffset(), indexInfo.getOffset());
            }
        }

        Assert.assertTrue(tmpFile.delete());
    }

    /*
    @Test
    @Ignore
    public void tmpKjPerf() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");
        try (PageAlignedReader reader = PageAlignedReader.open(tmpFile))
        {
            reader.setSegment(0);
            BirchReader<IndexInfo> birchReader = new BirchReader<>(reader);

            List<ClusteringPrefix> lookup = new ArrayList<>();
            List<String> gen = new ArrayList<>();
            gen.add("2efe952e-f7ac-11e5-9a30-0d228f97559b");
            gen.add("240f82fa-f7ac-11e5-9a30-0d228f97559b");
            gen.add("194a3f6f-f7ac-11e5-9a30-0d228f97559b");
            gen.add("0ebbc354-f7ac-11e5-9a30-0d228f97559b");
            gen.add("03cd7470-f7ac-11e5-9a30-0d228f97559b");
            gen.add("f85cfcc5-f7ab-11e5-9a30-0d228f97559b");
            gen.add("edca3b61-f7ab-11e5-9a30-0d228f97559b");
            gen.add("e3464696-f7ab-11e5-9a30-0d228f97559b");
            gen.add("d8a246ab-f7ab-11e5-9a30-0d228f97559b");
            gen.add("cdf03d77-f7ab-11e5-9a30-0d228f97559b");
            gen.add("c27bce2c-f7ab-11e5-9a30-0d228f97559b");
            gen.add("8b32d233-f7ab-11e5-9a30-0d228f97559b");

            for (String timeStr : gen)
            {
                UUID uuid = UUID.fromString(timeStr);
                SimpleCType.SimpleCBuilder builder = new SimpleCType.SimpleCBuilder(new SimpleDenseCellNameType(TimeUUIDType.instance));
                builder.add(uuid);
                lookup.add(builder.build());
            }

            // quick warm-up iterations..
            for (int i = 0; i < 5; i++)
            {
                for (ClusteringPrefix toLookup : lookup)
                {
                    IndexInfo person = birchReader.search(toLookup, new SimpleCType(TimeUUIDType.instance), false);
                }
            }

            AbstractType<?> type = CompositeType.getInstance(Arrays.asList(new AbstractType<?>[]{ TimeUUIDType.instance}));

            com.codahale.metrics.Timer timer = new com.codahale.metrics.Timer();
            for (int i = 0; i < 1000; i++)
            {
                for (ClusteringPrefix toLookup : lookup)
                {
                    final com.codahale.metrics.Timer.Context timerContext = timer.time();
                    IndexInfo indexInfo = birchReader.search(toLookup, type, false);
                    logger.info("birch returned {}:{}:{}", TimeUUIDType.instance.getString(indexInfo.getFirstName()),
                                indexInfo.getOffset(), indexInfo.getWidth());
                    timerContext.stop();
                }
            }

            logger.error("Timer ({} iterations) => 95th: {} 99th: {} 99.9th: {} mean: {} median: {}", timer.getCount(),
                         timer.getSnapshot().get95thPercentile(), timer.getSnapshot().get99thPercentile(),
                         timer.getSnapshot().get999thPercentile(), timer.getSnapshot().getMean(), timer.getSnapshot().getMedian());
        }

        Assert.assertTrue(tmpFile.delete());
    }
    */

    @Test(timeout = 30000)
    public void simpleTreeIterateStartingFromSearchOffset() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), ".birch");

        int sampleRate = 5000;
        Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(50000, sampleRate);

        try (PageAlignedWriter writer = new PageAlignedWriter(tmpFile))
        {
            writer.startNewSegment();
            writer.startNewNonPageAlignedSubSegment();
            String keyStr = generateRandomWord(RANDOM.nextInt(50) + 3);
            ByteBuffer key = ByteBuffer.wrap(keyStr.getBytes()).duplicate();
            writer.writeShort(key.capacity());
            writer.write(key);
            writer.finalizeCurrentSubSegment();

            BirchWriter birchWriter = new BirchWriter.Builder(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, cfs.getComparator()).build();

            writer.startNewSubSegment(birchWriter.getCacheLineSize());
            birchWriter.serialize(writer);
            writer.finalizeCurrentSubSegment();
            writer.finalizeCurrentSegment();
        }

        try (PageAlignedReader reader = PageAlignedReader.open(tmpFile))
        {
            reader.setSegment(0);
            reader.nextSubSegment();

            SerializationHeader serializationHeader = new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS);
            BirchReader birchReader = new BirchReader(reader, serializationHeader, CURRENT_VERSION);
            Assert.assertEquals(50000, birchReader.getElementCount());
            List<IndexInfo> samples = ((TimeUUIDTreeSerializableIterator) timeUUIDIterator).getSamples();
            Iterator<IndexInfo> iterator = birchReader.getIterator(samples.get(1).getFirstName(), null, cfs.metadata.get(), false);
            int iteratorCount = 0;
            while (iterator.hasNext())
            {
                IndexInfo next = iterator.next();
                iteratorCount++;
            }
            Assert.assertEquals(50000 - (sampleRate - 1), iteratorCount);
        }

        Assert.assertTrue(tmpFile.delete());
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
