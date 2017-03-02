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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import com.google.common.collect.AbstractIterator;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.IndexInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PageAlignedReader;
import org.apache.cassandra.metrics.BirchMetrics;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.io.sstable.birch.BirchWriter.SERIALIZERS;

/**
 * Provides logic to both search and iterate through a serialized Birch tree.
 *
 * Refer to {@link org.apache.cassandra.io.sstable.birch.BirchWriter} for documentation
 * on the Birch file format. A BirchReader is backed by a {@link PageAlignedReader}
 * which handles the underlying managment of memory mapping and reading from disk in
 * a cache-friendly/aligned way.
 * <p>
 * This class is <b>*not*</b> thread safe.
 *
 * @see BirchWriter
 */
public class BirchReader<T> implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(BirchReader.class);

    private PageAlignedReader reader;
    private final SerializationHeader header;
    private final Version version;
    private final Descriptor descriptor;
    private final BSerializer<T> serializer;

    public BirchReader(PageAlignedReader reader, SerializationHeader header, Version version) throws IOException
    {
        this.reader = reader;
        this.header = header;
        this.version = version;

        assert reader.isCurrentSubSegmentPageAligned();

        reader.seek(reader.getCurrentSubSegmentAlignedEndOffset() - reader.getPageAlignedChunkSize());
        this.descriptor = Descriptor.deserialize(reader);
        reader.seekToStartOfCurrentSubSegment();
        this.serializer = SERIALIZERS.get(descriptor.getSerializerType());
    }

    public void unsafeReplaceReader(PageAlignedReader reader)
    {
        this.reader = reader;
    }

    /**
     * @return the total number of elements available in this tree
     */
    public int getElementCount()
    {
        return descriptor.getElementCount();
    }

    /**
     * @param comparator an instance of ClusteringComparator to use
     * @param reversed if the iterator should iterate thru elements in either forwards or reversed direction
     * @return an instance of BirchIterator
     * @throws IOException thrown if a deserialization or IO error is encountered while creating the iterator
     */
    public BirchIterator getIterator(ClusteringComparator comparator, boolean reversed) throws IOException
    {
        return new BirchIterator(comparator, reversed);
    }

    /**
     * @param searchKey start the iteration of the iterator from this provided key
     * @param comparator an instance of ClusteringComparator to use
     * @param reversed if the iterator should iterate thru elements in either forwards or reversed direction
     * @return an instance of BirchIterator
     * @throws IOException thrown if a deserialization or IO error is encountered while creating the iterator
     */
    public BirchIterator getIterator(ClusteringPrefix searchKey, ClusteringComparator comparator, boolean reversed) throws IOException
    {
        return new BirchIterator(searchKey, comparator, reversed);
    }

    /**
     * Returns the key and raw file offset of the element in the tree at idx. If the
     * key has an overflow component, the key returned will be a fully reassembled ByteBuffer
     * containing both the part of the key in the tree and the remaining bytes deserialized
     * from the overflow page.
     *
     * @param idx the index of the element in the tree to return
     * @return a KeyAndOffsetPtr object that contains a ByteBuffer of the key for that
     *         element and the raw file offset where that key was found
     * @throws IOException thrown if an IOException is encountered while deserializing the key
     */
    private KeyAndOffsetPtr getKeyAndOffsetPtr(int idx) throws IOException
    {
        PageAlignedFileMark segmentStartMark = (PageAlignedFileMark) reader.mark();

        short entries = reader.readShort();

        // check if element has overflow length encoded after key
        int byteForIdxInOverflowField = Byte.BYTES * (int) (idx / 8.0);
        reader.seek(segmentStartMark.pointer + Short.BYTES
                    + ((entries * Short.BYTES) + Short.BYTES) + (entries * Long.BYTES) + byteForIdxInOverflowField);
        byte hasOverflowByte = reader.readByte();
        boolean hasOverflow = (hasOverflowByte >> (idx % 8) & 1) == 1;

        // Skip to internal offsets section for this element to find where to start reading the key.
        // Get this elements offset and elm + 1's offset. We calculate the length to read as
        // (elm + 1 offset) - (elm offset)
        reader.seek(segmentStartMark.pointer + Short.BYTES + (idx * Short.BYTES));
        short offsetInNodeToKey = reader.readShort();
        short offsetOfNextKey = reader.readShort();
        int lengthOfKey = offsetOfNextKey - offsetInNodeToKey;

        // skip into this segment past encoded elements and encoded key offsets. Then skip idx number of Longs
        // into the encoded offsets to get the offset for this element
        reader.seek(segmentStartMark.pointer + Short.BYTES + ((entries * Short.BYTES) + Short.BYTES) + (idx * Long.BYTES));
        long ptrOffset = reader.readLong();

        // skip to the starting offset of the key
        reader.seek(segmentStartMark.pointer + offsetInNodeToKey);

        ByteBuffer key = getKey(lengthOfKey, hasOverflow);

        // the reader makes the assumptions that previous operations will clean
        // up after themselves and will always reset the segment back to the start
        reader.reset(segmentStartMark);

        return new KeyAndOffsetPtr(key, ptrOffset);
    }

    /**
     * @param idx the index of the element in the tree to deserialize and return
     * @param comparator a ClusteringComparator instance to use
     * @return the object (of type T) found at the given index
     * @throws IOException thrown if an IOException is encountered while deserializing the element
     */
    private T getElement(int idx, ClusteringComparator comparator) throws IOException
    {
        PageAlignedFileMark segmentStartMark = (PageAlignedFileMark) reader.mark();

        short entries = reader.readShort();

        long startingOffsetsPosition = reader.getOffset();
        long valuesOffset = startingOffsetsPosition + ((entries + 1) * Short.BYTES);

        // check if this element has an overflow component
        int byteForIdxInOverflowField = Byte.BYTES * (int) (idx / 8.0);
        reader.seek(valuesOffset + (entries * serializer.serializedValueSize()) + byteForIdxInOverflowField);
        byte hasOverflowByte = reader.readByte();
        boolean hasOverflow = (hasOverflowByte >> (idx % 8) & 1) == 1;

        // calculate length of encoded key from the encoded offset of (elm + 1) - elm
        reader.seek(startingOffsetsPosition + (idx * Short.BYTES));
        short offsetInNodeToKey = reader.readShort();
        short offsetOfNextKey = reader.readShort();
        int lengthOfKey = offsetOfNextKey - offsetInNodeToKey;

        reader.seek(segmentStartMark.pointer + offsetInNodeToKey);

        ByteBuffer key = getKey(lengthOfKey, hasOverflow);

        reader.seek(valuesOffset + (idx * serializer.serializedValueSize()));
        T obj = serializer.deserializeValue(key, comparator, reader);

        // always reset back to offset we started with as reader makes assumptions
        // we'll always be at the start of a segment
        reader.reset(segmentStartMark);

        return obj;
    }

    /**
     * @param lengthOfKey the length of the bytes of the key encoded in the Birch node itself
     * @param hasOverflow if the key has an Overflow component
     * @return a ByteBuffer with the full contents of the key deserialized from the Index
     * @throws IOException an io error occured while reading the key from the Index file
     */
    private ByteBuffer getKey(int lengthOfKey, boolean hasOverflow) throws IOException
    {
        ByteBuffer key = (hasOverflow)
                         ? getKeyWithOverflow(lengthOfKey)
                         : getKeyWithoutOverflow(lengthOfKey);

        // always ensure we set the position() of the key we are returning to 0
        // so regardless of what happens, the caller can always assume it doesn't
        // need to reset the position before using it.
        key.position(0);

        BirchMetrics.totalBytesReadPerKey.update(key.limit());

        return key;
    }

    /**
     * @param lengthOfKey the number of bytes to read from the backing Index for this key
     * @return a ByteBuffer of the key containing the bytes read from the backing Index file
     * @throws IOException an io error occured while reading the key from the Index file
     */
    private ByteBuffer getKeyWithoutOverflow(int lengthOfKey) throws IOException
    {
        ByteBuffer key = reader.readBytes(lengthOfKey);
        return key;
    }

    /**
     *
     * @param lengthOfKey the length of the bytes of the key to read from the component inside
     *                    the Birch node itself
     * @return a reassembled single ByteBuffer containing the entire key with the bytes from
     *         both the Birch node and the overflow bytes as necessary
     * @throws IOException an io error occured while reading the key from the Index file
     */
    private ByteBuffer getKeyWithOverflow(int lengthOfKey) throws IOException
    {
        // if the key has an overflow component, remove one Long worth of bytes
        // from key length encoded in the birch node itself as calculated size
        // will include encoded overflow offset (if element has overflow component)
        lengthOfKey = lengthOfKey - Long.BYTES;

        BirchMetrics.readsRequiringOverflow.mark();

        DataPosition markBeforeOverflowSeek = reader.mark();

        // we have to skip over the key to get to the offset in the overflow page
        // we'll skip back and read it later so we can allocate a single buffer
        // instead of allocating one to get the bytes from the node we're skipping
        // initially here, one for the overflow bytes, and a final merged one for the two
        reader.skipBytes(lengthOfKey);

        // read relative overflow offset encoded at end of key
        long offsetInOverflowPage = reader.readLong();
        // skip to offset in overflow and get key's overflow bytes. then we
        // create a single merged byte buffer with the merged results of the
        // key bytes from the tree and it's overflow bytes
        reader.seek(descriptor.getOverflowPageOffset() + offsetInOverflowPage);
        int lengthToRead = reader.readInt();
        BirchMetrics.additionalBytesReadFromOverflow.update(lengthToRead);

        // create one byte[] that fits the entire key (both the part of the
        // key that fit in the birch node itself and the remaining overflow bits)
        byte[] reconstructedKeyBuf = new byte[lengthOfKey + lengthToRead];
        reader.readFully(reconstructedKeyBuf, lengthOfKey, lengthToRead);

        // seek the reader back to the offset in the file where the bytes for the
        // key that fit inside the birch leaf are
        reader.reset(markBeforeOverflowSeek);

        // read the first (and remaining) bytes for the key from the birch node
        // to fully reassembly the key
        reader.readFully(reconstructedKeyBuf, 0, lengthOfKey);
        return ByteBuffer.wrap(reconstructedKeyBuf);
    }

    /**
     * Returns a object found in the tree by performing a binary search within a leaf node
     *
     * @param searchKey the ClusteringPrefix to binary search the leaf for
     * @param comparator the ClusteringComparator instance to use
     * @param reversed if searchKey contains an empty ByteBuffer, reversed is used to determine if
     *                 we should return the first (or last) element
     * @return a Pair, where the left is the object to return and right is the
     *         index of element in the tree being returned
     * @throws IOException thrown if an IOException is encountered while searching the leaf
     */
    private Pair<T, Integer> binarySearchLeaf(ClusteringPrefix searchKey, ClusteringComparator comparator, boolean reversed) throws IOException
    {
        PageAlignedFileMark nodeStartingMark = (PageAlignedFileMark) reader.mark();

        short entries = reader.readShort();

        int startIdx = 0;
        int endIdx = entries - 1;

        if (searchKey.dataSize() == 0)
        {
            if (reversed)
            {
                Pair<T, Integer> ret = Pair.create(getElement((int) entries - 1, comparator), (int) entries - 1);
                reader.reset(nodeStartingMark);
                return ret;
            }
            else
            {
                return Pair.create(getElement(0, comparator), 0);
            }
        }

        int index = binarySearch(searchKey, comparator, nodeStartingMark, startIdx, endIdx);
        //int indexRet = (index < 0 ? -index - 1 : index);
        int indexRet = index;
        if (indexRet < 0 || indexRet >= entries)
        {
            indexRet = (reversed) ? entries - 1 : 0;
        }
        //indexRet = (reversed) ? entries : -1;
        logger.info("binarySearchLeaf ==> index: {} indexRet: {}", index, indexRet);
        T elm = (indexRet < 0 || indexRet >= entries) ? null : getElement(indexRet, comparator);

        reader.reset(nodeStartingMark);

        return Pair.create(elm, indexRet);
    }

    /*
    private Pair<T, Integer> binarySearchLeaf(ClusteringPrefix searchKey, ClusteringComparator comparator, boolean reversed) throws IOException
    {
        PageAlignedFileMark nodeStartingMark = (PageAlignedFileMark) reader.mark();

        if (searchKey.dataSize() == 0)
        {
            if (reversed)
            {
                short entries = reader.readShort();
                Pair<T, Integer> ret = Pair.create(getElement((int) entries - 1, comparator), (int) entries - 1);
                reader.reset(nodeStartingMark);
                return ret;
            }
            else
            {
                return Pair.create(getElement(0, comparator), 0);
            }
        }

        short entries = reader.readShort();

        T ret = null;
        int retIdx = -1;

        int start = 0;
        int end = entries - 1; // binary search is zero-indexed
        int middle = (end - start) / 2;

        while (start <= end)
        {
            reader.reset(nodeStartingMark);

            T elm = getElement(middle, comparator);

            ByteBuffer key = ((TreeSerializable) elm).serializedKey(comparator);
            ClusteringPrefix keyPrefix = ClusteringPrefix.serializer.deserialize(key,
                                                                                 version.correspondingMessagingVersion(),
                                                                                 header.clusteringTypes());

            int cmp = comparator.compare(keyPrefix, searchKey);
            if (cmp == 0)
            {
                return Pair.create(elm, middle);
            }

            if (cmp < 0)
            {
                ret = elm;
                retIdx = middle;
                start = middle + 1;
            }
            else
            {
                end = middle - 1;
            }
            middle = (start + end) / 2;
        }

        reader.reset(nodeStartingMark);

        return Pair.create(ret, retIdx);
    }
*/
    private int binarySearch(ClusteringPrefix searchKey, ClusteringComparator comparator,
                             PageAlignedFileMark nodeStartingMark, int low, int high) throws IOException
    {
        logger.info("binarySearch low: {} high: {}", low, high);
        while (low <= high)
        {
            reader.reset(nodeStartingMark);

            int mid = (low + high) >>> 1;

            T elm = getElement(mid, comparator);
            ByteBuffer midKey = ((TreeSerializable) elm).serializedKey(comparator);
            ClusteringPrefix midValPrefix = ClusteringPrefix.serializer.deserialize(midKey,
                                                                                 version.correspondingMessagingVersion(),
                                                                                 header.clusteringTypes());

            int cmp = comparator.compare(midValPrefix, searchKey);
            logger.info("binary search loop... cmp res: {}", cmp);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }

    /**
     * Perform a binary search over an inner-node to return the offset
     * in the file for the leaf node that should contain searchKey
     *
     * @param searchKey the ClusteringPrefix to binary search for. If empty, this method will return the
     *                  first (or last) offset depending on the value of reversed
     * @param comparator an instance of ClusteringComparator
     * @param reversed if searchKey contains an empty ByteBuffer, reversed is used to determine if
     *                 we should return the offset of the first or last leaf
     * @return file offset that matches search
     * @throws IOException failure while reading or deserializing the index file
     */
    private long binarySearchNode(ClusteringPrefix searchKey, ClusteringComparator comparator, boolean reversed) throws IOException
    {
        assert searchKey != null;

        PageAlignedFileMark nodeStartingMark = (PageAlignedFileMark) reader.mark();

        if (searchKey.dataSize() == 0)
        {
            if (reversed)
            {
                short entries = reader.readShort();
                KeyAndOffsetPtr keyAndOffsetPtr = getKeyAndOffsetPtr(entries - 1);
                reader.reset(nodeStartingMark);
                return keyAndOffsetPtr.offsetPtr;
            }
            else
            {
                return getKeyAndOffsetPtr(0).offsetPtr;
            }
        }

        short entries = reader.readShort();

        long retOffset = -1;

        int start = 0;
        int end = entries - 1; // binary search is zero-indexed
        int middle = (start + end) / 2;

        while (start <= end)
        {
            reader.reset(nodeStartingMark);

            KeyAndOffsetPtr keyAndOffsetPtr = getKeyAndOffsetPtr(middle);

            if (entries == 1)
            {
                retOffset = keyAndOffsetPtr.offsetPtr;
                break;
            }

            ClusteringPrefix keyPrefix = ClusteringPrefix.serializer.deserialize(keyAndOffsetPtr.key,
                                                                                 version.correspondingMessagingVersion(),
                                                                                 header.clusteringTypes());

            int cmp = comparator.compare(keyPrefix, searchKey);
            if (cmp == 0)
            {
                retOffset = keyAndOffsetPtr.offsetPtr;
                break;
            }

            if (cmp < 0)
            {
                retOffset = keyAndOffsetPtr.offsetPtr;
                start = middle + 1;
            }
            else
            {
                end = middle - 1;
            }
            middle = (start + end) / 2;
        }

        return retOffset;
    }

    /**
     * Search the Birch tree for a specific key (and return) an instance of T
     * with the value deserialized from the matching element in the tree.
     *
     * @param searchKey the ClusteringPrefix to search for
     * @param comparator an instnace of ClusteringComparator to use for the ClusteringPrefix
     * @param reversed if the search logic should iterate on the tree in forwards or reversed order
     *
     * @return an instance of T matching the searchKey
     * @throws IOException thrown if an IOException is encountered while searching the tree
     */
    public T search(ClusteringPrefix searchKey, ClusteringComparator comparator, boolean reversed) throws IOException
    {
        try (Timer.Context timerContext = BirchMetrics.totalTimeSpentPerSearch.time())
        {
            long offset = descriptor.getRootOffset();

            while (offset >= descriptor.getFirstNodeOffset())
            {
                reader.seek(offset);
                offset = binarySearchNode(searchKey, comparator, reversed);
                if (offset == -1)
                {
                    // todo: null is lame, use Optional instead?
                    return null;
                }
            }

            reader.seek(offset); // go to leaf node that we will return a result from

            Pair<T, Integer> res = binarySearchLeaf(searchKey, comparator, reversed);
            return res.left;
        }
    }

    private class KeyAndOffsetPtr
    {
        private final ByteBuffer key;
        private final long offsetPtr;

        public KeyAndOffsetPtr(ByteBuffer key, long offsetPtr)
        {
            this.key = key;
            this.offsetPtr = offsetPtr;
        }
    }

    /**
     * An Iterator implementation that can iterate either forwards or
     * reversed thru all elements, and optionally start the iteration
     * from a given provided start element.
     */
    public class BirchIterator extends AbstractIterator<T>
    {
        private final ClusteringComparator comparator;
        private final boolean reversed;

        private final long totalSizeOfLeafs;
        private final int totalLeafPages;

        private int currentPage;
        private int currentElmIdx;

        /**
         * Returns a new instance of BirchIterator for iterating either
         * forwards or reversed thru all elements in a Birch Tree, starting
         * the iteration from a specific element in the tree.
         *
         * If reversed, the first element returned will be the very last
         * element. If not-reversed, the first element returned will be the
         * first element in the tree.
         *
         * @param searchKey the ClusteringPrefix to start the iterator's iteration from
         * @param comparator the ClusteringComparator instance to use for the Composite elements in the tree
         * @param reversed if the tree should iterate over elements forwards or reversed
         * @throws IOException thrown if a IOException is encountered while creating the iterator
         */
        public BirchIterator(ClusteringPrefix searchKey, ClusteringComparator comparator, boolean reversed) throws IOException
        {
            logger.info("at the top of BirchIterator constructor.. is reversed? {}", reversed);

            this.comparator = comparator;
            this.reversed = reversed;
            this.totalSizeOfLeafs = descriptor.getFirstNodeOffset() - descriptor.getFirstLeafOffset();
            this.totalLeafPages = (int) (totalSizeOfLeafs / descriptor.getAlignedPageSize());

            if (1 == 2 && searchKey != null && searchKey.dataSize() > 0)
            {
                logger.info("BirchIterator constructor was given a non-null search key! is reversed? {}", reversed);

                // find the element closest to the search key and start the iteration from there
                long offset = descriptor.getRootOffset();

                while (offset >= descriptor.getFirstNodeOffset())
                {
                    reader.seek(offset);
                    offset = binarySearchNode(searchKey, comparator, reversed);
                }

                if (offset == -1)
                {
                    // search key wasn't found, start at either the first or last element
                    // depending on if the iterator is in reversed mode or not
                    initializeIteratorToDefaultStart();
                }
                else
                {
                    this.currentPage = (int) (offset - descriptor.getFirstLeafOffset()) / descriptor.getAlignedPageSize();

                    // go to leaf...
                    reader.seek(offset);

                    int binarySearchRes = binarySearchLeaf(searchKey, comparator, reversed).right;
                    //this.currentElmIdx = (binarySearchRes <= 0) ? -1 : binarySearchRes;

                    logger.info("kjabc updating currentElmIdx {} ==> {}", this.currentElmIdx, binarySearchRes);
                    //this.currentElmIdx = (reversed) ? binarySearchRes + 1 : binarySearchRes - 1;
                    this.currentElmIdx = (reversed) ? binarySearchRes : binarySearchRes;
                }
            }
            else
            {
                logger.info("BirchIterator constructor called with a null search key.. going to call initializeIteratorToDefaultStart");
                initializeIteratorToDefaultStart();
            }
        }

        private void initializeIteratorToDefaultStart() throws IOException
        {
            logger.info("at the top of initializeIteratorToDefaultStart");
            // traverse and return all elements in the tree
            if (reversed)
            {
                this.currentPage = totalLeafPages;
                // as we are in reversed mode, go to the last leaf page
                reader.seek(descriptor.getFirstLeafOffset() + ((currentPage - 1) * (descriptor.getAlignedPageSize())));
                PageAlignedFileMark currentPageStart = (PageAlignedFileMark) reader.mark();
                short numElements = reader.readShort();
                logger.info("numElements for reversed init in initializeIteratorToDefaultStart is {}", numElements);
                //this.currentElmIdx = (numElements == 1) ? 0 : numElements - 1;
                logger.info("updating currentElmIdx {} ==> {}", currentElmIdx, numElements - 1);
                this.currentElmIdx = numElements - 1;
                reader.reset(currentPageStart);
            }
            else
            {
                this.currentPage = 0;
                this.currentElmIdx = 0;
            }
        }

        /**
         * Returns a new instance of BirchIterator for iterating either
         * forwards or reversed thru all elements in a Birch Tree.
         *
         * If reversed, the first element returned will be the very last
         * element. If not-reversed, the first element returned will be the
         * first element in the tree.
         *
         * @param comparator the ClusteringComparator instance
         * @param reversed if the iterator should iterate forwards or backwards
         * @throws IOException thrown if an IOException is encountered while iterating the tree
         */
        public BirchIterator(ClusteringComparator comparator, boolean reversed) throws IOException
        {
            this(null, comparator, reversed);
        }

        public T computeNext()
        {
            logger.info("at the top of computeNext() ==> currentElmIdx: {} currentPage: {} reversed: {}", currentElmIdx, currentPage, reversed);
            try
            {
                if (reversed)
                {
                    //if (currentPage - 1 < 0 && currentElmIdx < 0)
                      //  return endOfData();

                    int newCurrentPage = (currentPage == 0) ? 0 : currentPage - 1;
                    //int newCurrentPage = currentPage - 1;
                    logger.info("kjisreversed and currentPage: {} currentElmIdx: {}", currentPage, currentElmIdx);
                    //if (newCurrentPage < 0 && currentElmIdx <= 1)
                      //  return endOfData();
                   // if (currentPage <= 0)

                    //if (newCurrentPage < 0)
                    //    newCurrentPage = 0;
                    //assert newCurrentPage >= 0;
                    //newCurrentPage = currentPage;
                    //if (currentElmIdx >= 0)
                        reader.seek(descriptor.getFirstLeafOffset() + (newCurrentPage * (descriptor.getAlignedPageSize())));
                    //reader.seek(descriptor.getFirstLeafOffset() + ((currentPage - 1) * (descriptor.getAlignedPageSize())));
                }
                else
                {
                    reader.seek(descriptor.getFirstLeafOffset() + (currentPage * (descriptor.getAlignedPageSize())));
                }

                PageAlignedFileMark leafOffsetStart = (PageAlignedFileMark) reader.mark();

                short numElements = reader.readShort();
                logger.info("in computeNext() reader.getOffset() {} currentPage {} currentElmIdx {} reversed {} numElements {} reversed {}",
                            reader.getOffset(), currentPage, currentElmIdx, reversed, numElements, reversed);

                if (!reversed && currentElmIdx >= numElements)
                {
                    // we have iterated over all elements in the current page
                    // check if we have any other pages to consume from next
                    if (currentPage + 1 >= totalLeafPages)
                    {
                        // we have exhausted all elements in all pages
                        return endOfData();
                    }
                    else
                    {
                        currentPage++;
                        currentElmIdx = 0;
                        reader.seek(descriptor.getFirstLeafOffset() + (currentPage * (descriptor.getAlignedPageSize())));
                        leafOffsetStart = (PageAlignedFileMark) reader.mark();
                    }
                }
                else if (reversed && currentElmIdx < 0)
                {
                    // if currentElmIdx is <= 0, we've already iterated in the past and returned the last element,
                    // which, because we're in reversed mode, is the first element in the current page. We need
                    // to check if we have more pages that we need to iterate thru (if so we switch to the previous
                    // page and reset our currentElmIdx to the number of elements serialized in the new page),
                    // and then start iterating from the end to the front again. If we're on the last element in
                    // the page (technically this will be the first element but given we're in reversed mode that is
                    // the last), then we've done and have iterated thru all elements and should now return endOfData()
                    logger.info("kjabc123 reversed: {} currentElmIdx: {} currentPage: {}", reversed, currentElmIdx, currentPage);
                    // we have iterated over all elements in the current page
                    // check if we have any other pages to consume from next
                    if (currentPage - 1 <= 0)
                    {
                        // we have exhausted all elements in all pages
                        return endOfData();
                    }
                    else
                    {
                        currentPage--;
                        reader.seek(descriptor.getFirstLeafOffset() + (currentPage * (descriptor.getAlignedPageSize())));
                        leafOffsetStart = (PageAlignedFileMark) reader.mark();
                        numElements = reader.readShort();
                        currentElmIdx = numElements - 1;
                        reader.reset(leafOffsetStart);
                    }
                }

                int elmIdx = currentElmIdx;
                /*
                int elmIdx = (reversed) ? currentElmIdx - 1 : currentElmIdx + 1;
                if (!(elmIdx >= 0 && elmIdx <= numElements)) {
                    logger.error("idx: {} not within valid bounds {} <--> {}", elmIdx, -1, numElements);
                    return endOfData();
                }
                assert elmIdx >= -1 && elmIdx <= numElements : String.format("idx: %d not within valid bounds " +
                                                                               "%d <--> %d", elmIdx, -1, numElements);
                                                                               */

                reader.seek(leafOffsetStart.pointer);
                T next = getElement(elmIdx, comparator);

                if (reversed)
                    currentElmIdx--;
                else
                    currentElmIdx++;

                return next;
            }
            catch (IOException e)
            {
                logger.error("kjk fuck", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close()
    {
        logger.info("BirchReader#close()");
        FileUtils.closeQuietly(reader);
    }
}
