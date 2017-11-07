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

package org.apache.cassandra.io.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.AbstractIterator;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.birch.*;

import static org.apache.cassandra.io.sstable.birch.PageAlignedWriter.SEGMENT_ONLY_HAS_SINGLE_ALIGNED_SUBSEGMENT;
import static org.apache.cassandra.io.sstable.birch.PageAlignedWriter.SEGMENT_ONLY_HAS_SINGLE_NON_ALIGNED_SUBSEGMENT;
import static org.apache.cassandra.io.sstable.birch.PageAlignedWriter.SIZE_FIXED_LENGTH_COMPONENTS_SINGLE_SEGMENT;
import static org.apache.cassandra.io.sstable.birch.PageAlignedWriter.SIZE_FIXED_LENGTH_COMPONENTS_SINGLE_SUB_SEGMENT;
import static org.apache.cassandra.io.sstable.birch.PageAlignedWriter.SIZE_SEGMENT_HEADER_SERIALIZATION_OVERHAED;

/**
 * Reads an aligned and segmented file. Documentation on the
 * the binary format used to serialize compatable files is available in the
 * {@link org.apache.cassandra.io.sstable.birch.PageAlignedWriter} docs.
 * <p>
 * Instances of this class are <b>not</b> thread safe.
 *
 * @see PageAlignedWriter
 */
public class PageAlignedReader extends RebufferingInputStream implements FileDataInput, Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(PageAlignedReader.class);

    //private final File file;
    //private final org.apache.cassandra.io.sstable.birch.BufferingPageAlignedReader raf;

    // The default buffer size when the client doesn't specify it
    public static final int DEFAULT_BUFFER_SIZE = 4096;

    // offset of the last file mark
    private long markedPointer;

    final Rebufferer rebufferer;
    private Rebufferer.BufferHolder bufferHolder = Rebufferer.EMPTY;

    private int totalSegments;
    private long serializedDescriptorsStartingOffset;
    private int relativeOffsetToSerializedAlignedSegmentObjects;
    private long maxOffset;
    private int pageAlignedChunkSize;

    private int currentSegmentIdx;
    private long currentSegmentOffset;
    private long currentSegmentUsableEndOffset;
    private long currentSegmentAlignedEndOffset;

    private short currentSubSegmentIdx;
    private short totalSubSegmentsInCurrentSegment;
    private long currentSubSegmentOffset;
    private long currentSubSegmentUsableEndOffset;
    private long currentSubSegmentAlignedEndOffset;
    private boolean currentSubSegmentIsPageAligned;

    private Exception closeException = null;
    private boolean isClosed = false;

    /**
     * Only created through Builder
     *
     * @param rebufferer Rebufferer to use
     * @param skipSetup if we should skip the setup logic of this reader
     */
    PageAlignedReader(Rebufferer rebufferer, boolean skipSetup) throws IOException
    {
        super(Rebufferer.EMPTY.buffer());
        this.rebufferer = rebufferer;

        //this.raf = new org.apache.cassandra.io.sstable.birch.BufferingPageAlignedReader(file);
        if (!skipSetup)
            setup();
    }

    public static PageAlignedReader copy(PageAlignedReader src) throws IOException
    {
        PageAlignedReader newReader = open(new File(src.getPath()), true);
        newReader.totalSegments = src.totalSegments;
        newReader.serializedDescriptorsStartingOffset = src.serializedDescriptorsStartingOffset;
        newReader.relativeOffsetToSerializedAlignedSegmentObjects = src.relativeOffsetToSerializedAlignedSegmentObjects;
        newReader.maxOffset = src.maxOffset;
        newReader.pageAlignedChunkSize = src.pageAlignedChunkSize;

        newReader.currentSegmentIdx = src.currentSegmentIdx;
        newReader.currentSegmentOffset = src.currentSegmentOffset;
        newReader.currentSegmentUsableEndOffset = src.currentSegmentUsableEndOffset;
        newReader.currentSegmentAlignedEndOffset = src.currentSegmentAlignedEndOffset;

        newReader.currentSubSegmentIdx = src.currentSubSegmentIdx;
        newReader.totalSubSegmentsInCurrentSegment = src.totalSubSegmentsInCurrentSegment;
        newReader.currentSubSegmentOffset = src.currentSubSegmentOffset;
        newReader.currentSubSegmentUsableEndOffset = src.currentSubSegmentUsableEndOffset;
        newReader.currentSubSegmentAlignedEndOffset = src.currentSubSegmentAlignedEndOffset;
        newReader.currentSubSegmentIsPageAligned = src.currentSubSegmentIsPageAligned;

        newReader.seek(newReader.currentSubSegmentOffset);
        //newReader.seek(src.getFilePointer());

        return newReader;
    }

    private void setup() throws IOException
    {
        seekInternal(length() - Long.BYTES);
        logger.debug("after first seekInternal in setup()... seek was to {}... we are actually at {} filePointer: {}", length() - Long.BYTES, current(), getFilePointer());
        this.serializedDescriptorsStartingOffset = readLong();
        seekInternal(serializedDescriptorsStartingOffset);
        this.totalSegments = readInt();
        this.relativeOffsetToSerializedAlignedSegmentObjects = readInt(); // relative starting offset of serialized segments
        this.maxOffset = readLong();
        this.pageAlignedChunkSize = readInt();

        logger.debug("setup(): serializedDescriptorsStartingOffset: {} totalSegments:{} relativeOffsetToSerializedAlignedSegmentObjects: {} maxOffset: {} pageAlignedChunkSize: {}", serializedDescriptorsStartingOffset, totalSegments, relativeOffsetToSerializedAlignedSegmentObjects, maxOffset, pageAlignedChunkSize);
        // todo kjkj fuck: commenting out for now as not sure how this works with the Chunk stuff
        //raf.updateBufferSize(pageAlignedChunkSize);

        setCurrentBoundsForSegment(0, 0);

        seek(0);
    }

    private void setCurrentBoundsForSegment(AlignedSegment alignedSegment, int subSegmentIdx) throws IOException
    {
        currentSegmentIdx = alignedSegment.idx;
        currentSegmentOffset = alignedSegment.offset;
        currentSegmentUsableEndOffset = alignedSegment.offset + alignedSegment.length;
        currentSegmentAlignedEndOffset = alignedSegment.offset + alignedSegment.alignedLength;

        AlignedSubSegment alignedSubSegment = alignedSegment.getSubSegments().get(subSegmentIdx);
        currentSubSegmentIdx = alignedSubSegment.idx;
        totalSubSegmentsInCurrentSegment = (short) alignedSegment.getSubSegments().size();
        currentSubSegmentOffset = alignedSubSegment.offset;
        currentSubSegmentUsableEndOffset = alignedSubSegment.offset + alignedSubSegment.length;
        currentSubSegmentAlignedEndOffset = alignedSubSegment.offset + alignedSubSegment.alignedLength;
        currentSubSegmentIsPageAligned = alignedSubSegment.isPageAligned;

        //seek(currentSubSegmentOffset); // todo: kj... should't be here because of BigTableScanner
    }

    private void setCurrentBoundsForSegment(int segmentIdx, int subSegmentIdx) throws IOException
    {
        AlignedSegment alignedSegment = getSegmentPointers(segmentIdx);
        setCurrentBoundsForSegment(alignedSegment, subSegmentIdx);
    }

    private AlignedSegment getSegmentPointers(int segmentIdx) throws IOException
    {
        long originalOffset = getOffset();

        // skip the fixed components (e.g. total segments, relative offset to variable length stuff, things we need in setup())
        // and then skip in SIZE_FIXED_LENGTH_COMPONENTS_SINGLE_SEGMENT times the segment we want to get to the start
        // of the fixed length components serialized for that segment id
        seekInternal(serializedDescriptorsStartingOffset
                           + SIZE_SEGMENT_HEADER_SERIALIZATION_OVERHAED
                           + (SIZE_FIXED_LENGTH_COMPONENTS_SINGLE_SEGMENT * segmentIdx));

        // the starting offset into the file for this segment
        long segmentOffset = readLong();
        // the total usable length of all segments (excluding any possible padding for alignment on page boundaries)
        long totalUsableLength = readLong();
        // the total length (including any possible padding) of a segment
        long totalAlignedLength = readLong();
        // the relative offset to start reading at to deserialize variable length parts for segment's sub-segments
        int relativeOffsetForThisSegmentsVariableLengthComponents = readInt();

        List<AlignedSubSegment> subSegments = new ArrayList<>();
        if (relativeOffsetForThisSegmentsVariableLengthComponents == SEGMENT_ONLY_HAS_SINGLE_ALIGNED_SUBSEGMENT
                || relativeOffsetForThisSegmentsVariableLengthComponents == SEGMENT_ONLY_HAS_SINGLE_NON_ALIGNED_SUBSEGMENT)
        {
            // we only have a single sub-segment in this segment, so use the "global" data deserialized
            // for the segment as the same offsets and length of the "sub-segment"
            boolean subSegmentIsAligned = relativeOffsetForThisSegmentsVariableLengthComponents == SEGMENT_ONLY_HAS_SINGLE_ALIGNED_SUBSEGMENT;
            subSegments.add(new AlignedSubSegment((short) 0, segmentOffset, totalUsableLength, totalAlignedLength, subSegmentIsAligned));
        }
        else
        {
            logger.debug("before seekInternal: serializedDescriptorsStartingOffset: {} relativeOffsetToSerializedAlignedSegmentObjects: {} relativeOffsetForThisSegmentsVariableLengthComponents: {}", serializedDescriptorsStartingOffset, relativeOffsetToSerializedAlignedSegmentObjects, relativeOffsetForThisSegmentsVariableLengthComponents);
            seekInternal(serializedDescriptorsStartingOffset
                     + relativeOffsetToSerializedAlignedSegmentObjects
                     + relativeOffsetForThisSegmentsVariableLengthComponents);

            short totalSubSegments = readShort();
            long startingOffsetForSubSegmentComponents = getOffset();
            for (int i = 0; i < totalSubSegments; i++)
            {
                seekInternal(startingOffsetForSubSegmentComponents
                         + (SIZE_FIXED_LENGTH_COMPONENTS_SINGLE_SUB_SEGMENT * i));

                // starting offset for this sub-segment
                long subSegmentOffset = readLong();
                // the length of data for this sub-segment (excluding any padding for page alignment)
                long subSegmentUsableLength = readLong();
                // the length of the data serialized for this sub-segment including any possible padding for page alignment
                long subSegmentAlignedLength = readLong();
                // if the sub-segment was serialized out as page aligned (thus possibly padded to an aligned boundry)
                boolean subSegmentIsPageAligned = readBoolean();
                subSegments.add(new AlignedSubSegment((short) i, subSegmentOffset, subSegmentUsableLength,
                                                      subSegmentAlignedLength, subSegmentIsPageAligned));
            }
        }

        // always return the offset for the file pointer on the raf back to the same position
        // it was in when the method was originally called
        seekInternal(originalOffset);

        return new AlignedSegment(segmentIdx, segmentOffset, totalUsableLength, totalAlignedLength, subSegments);
    }

    protected long getNextAlignedOffset(long offset)
    {
        // if offset <= alignTo, return 0
        // if next aligned offset > offset, return aligned offset - alignTo
        int alignTo = pageAlignedChunkSize;
        if (alignTo == 0 || offset == alignTo)
            return offset;
        if (offset <= alignTo)
            return 0;
        long alignedOffset = (offset + alignTo - 1) & ~(alignTo - 1);
        return (alignedOffset > offset) ? alignedOffset - alignTo : alignedOffset;
    }

    /**
     * @return total number of Aligned Segments encoded in this file
     */
    public int numberOfSegments()
    {
        return totalSegments;
    }

    /**
     * @return ttal number of aligned sub-segments in the current segment
     */
    public short numberOfSubSegments()
    {
        return totalSubSegmentsInCurrentSegment;
    }

    /**
     * @return a new instance of AlignedSegmentIterator
     */
    public AlignedSegmentIterator getSegmentIterator()
    {
        return new AlignedSegmentIterator();
    }

    public AlignedSegmentIterator getSegmentIterator(int startingSegment)
    {
        return new AlignedSegmentIterator(startingSegment);
    }

    /**
     * Returns an Iterator that will return all AlignedSegments in this PageAligned
     * file. Each returned AlignedSegment is lazily constructed as needed.
     */
    public class AlignedSegmentIterator extends AbstractIterator<AlignedSegment>
    {
        private int currentSegment;

        public AlignedSegmentIterator(int startingSegment)
        {
            this.currentSegment = startingSegment;
        }

        public AlignedSegmentIterator()
        {
            this.currentSegment = 0;
        }

        @Override
        public AlignedSegment computeNext()
        {
            if (currentSegment + 1 > totalSegments)
            {
                return endOfData();
            }

            try
            {
                return getSegmentPointers(currentSegment++);
            }
            catch (IOException e)
            {
                logger.error("Failed to deserialize aligned segment [idx {} of {}] from {}", currentSegment,
                             totalSegments, getPath());
                throw new RuntimeException(e);
            }
        }
    }

    public void setSegment(int segmentId) throws IOException
    {
        setSegment(segmentId, 0);
    }

    public void setSegment(int segmentId, int subSegmentId) throws IOException
    {
        assert segmentId < totalSegments;

        setCurrentBoundsForSegment(segmentId, subSegmentId);
        seek(currentSubSegmentOffset);
    }

    public void nextSubSegment() throws IOException
    {
        setSegment(currentSegmentIdx, currentSubSegmentIdx + 1);
    }

    public boolean hasNextSubSegment()
    {
        return currentSubSegmentIdx + 1 < totalSubSegmentsInCurrentSegment;
    }

    public boolean hasNextSegment()
    {
        return currentSegmentIdx + 1 < totalSegments;
    }

    public boolean hasPreviousSegment()
    {
        return currentSegmentIdx - 1 >= 0;
    }

    public void previousSegment() throws IOException
    {
        setSegment(currentSegmentIdx - 1);
    }

    public void nextSegment() throws IOException
    {
        setSegment(currentSegmentIdx + 1);
    }

    public long getOffset()
    {
        return bufferHolder.offset() + buffer.position();
    }

    public String getPath()
    {
        return getChannel().filePath();
    }

    public long length()
    {
        return rebufferer.fileLength();
    }

    public boolean isEOF() throws IOException
    {
        if (isClosed())
            return true;

        boolean isOutOfSegments = currentSegmentIdx + 1 >= totalSegments;
        boolean isOutOfSubSegments = currentSubSegmentIdx + 1 >= totalSubSegmentsInCurrentSegment;
        boolean currentSubSegmentExausted = getOffset() >= currentSubSegmentUsableEndOffset;
        boolean tmp = isOutOfSegments && isOutOfSubSegments && currentSubSegmentExausted;
        logger.debug("isEOF() called... isEOF()? {}", tmp);
        return tmp;
    }

    public boolean isCurrentSegmentExausted() throws IOException
    {
        logger.debug("isCurrentSegmentExausted() called... getOffset() {} >= currentSegmentUsableEndOffset {}", getOffset(), currentSegmentUsableEndOffset);
        return getOffset() >= currentSegmentUsableEndOffset;
    }

    public boolean isPositionInsideCurrentSegment(long pos)
    {
        return pos >= currentSegmentOffset && pos <= currentSegmentAlignedEndOffset;
    }

    public boolean isPositionInsideCurrentSubSegment(long pos)
    {
        return pos >= currentSubSegmentOffset && pos <= currentSubSegmentAlignedEndOffset;
    }

    public long bytesRemaining() throws IOException
    {
        return currentSegmentAlignedEndOffset - getOffset();
    }

    private void findAndSetCurrentSegmentAndSubSegmentForPosition(long pos) throws IOException
    {
        // given input position not within the current segment, so
        // binary search for the segment that contains the input position
        int segmentIdxContainingPos = findIdxForPosition(pos);
        setCurrentSubSegmentForPositionWithKnownSegmentIdx(pos, segmentIdxContainingPos);
    }

    private void setCurrentSubSegmentForPositionWithKnownSegmentIdx(long pos, int segmentIdx) throws IOException
    {
        AlignedSegment alignedSegment = getSegmentPointers(segmentIdx);

        int subSegmentIdx = 0;
        if (alignedSegment.getSubSegments().size() > 1)
        {
            for (AlignedSubSegment subSegment : alignedSegment.getSubSegments())
            {
                if (subSegment.isPositionInSubSegment(pos))
                {
                    subSegmentIdx = subSegment.idx;
                    break;
                }
            }
        }

        setCurrentBoundsForSegment(alignedSegment, subSegmentIdx);
    }

    /**
     * Scans thru all segments and subsegments and does a bounds check for the provided position.
     * When a segment + sub segment combination is found that contains the given position, the
     * pointers for both the current segment and subsegment are updated to that segment.
     *
     * Every effort should be made to avoid this check as it's obviously not optimal, but if
     * PageAlignedReader instances are reused, it might be required that we use it to re-prepare
     * the PageAlignedReader instance. A good example of where this is required is in
     * {@link org.apache.cassandra.io.sstable.format.big.BigTableScanner#seekToCurrentRangeStart()}
     *
     * @param pos absolute position in the data file to find the segment and subsegment bounds for
     * @throws IOException thrown if an exception is encountered while mapping the found region
     */
    public void findAndSetSegmentAndSubSegmentCurrentForPosition(long pos) throws IOException
    {
        assert pos >= 0 && pos <= maxOffset;

        if (pos < currentSegmentOffset || pos > currentSegmentUsableEndOffset)
        {
            findAndSetCurrentSegmentAndSubSegmentForPosition(pos);
            seek(pos);
        }

        if (pos > currentSubSegmentUsableEndOffset || pos < currentSubSegmentOffset
                                                        && (pos >= currentSegmentOffset && pos <= currentSegmentUsableEndOffset))
        {
            setCurrentSubSegmentForPositionWithKnownSegmentIdx(pos, currentSegmentIdx);
            seek(pos);
        }
    }

    /**
     * Performs a Binary Search across the fixed length encoded components
     * of the segments on disk.
     *
     * @param pos the file position to determine the index of the aligned segment
     *            that contains it
     * @return the index of the AlignedSegment that contains the provided position
     */
    public int findIdxForPosition(long pos) throws IOException
    {
        if (pos > maxOffset)
            throw new IOException("Invalid Position Provided. pos " + pos + " > maxOffset " + maxOffset);

        // optimization to avoid doing any of this work at all for pos 0
        if (pos == 0)
            return 0;

        long markedPos = getOffset();

        int retIdx = -1;

        int start = 0;
        int end = totalSegments - 1; // binary search is zero-indexed
        int middle = (end - start) / 2;

        while (start <= end)
        {
            // skip to the start of the fixed serialized components for the current 'middle' element
            seekInternal(serializedDescriptorsStartingOffset
                     + SIZE_SEGMENT_HEADER_SERIALIZATION_OVERHAED
                     + (middle * SIZE_FIXED_LENGTH_COMPONENTS_SINGLE_SEGMENT));

            long offset = readLong();

            int cmp = Long.compare(offset, pos);
            if (cmp == 0)
            {
                return middle;
            }

            if (cmp < 0)
            {
                retIdx = middle;
                start = middle + 1;
            }
            else
            {
                end = middle - 1;
            }
            middle = (start + end) / 2;
        }

        seek(markedPos);

        return retIdx;
    }

    private void seekInternal(long pos) throws IOException
    {
        logger.debug("seekInternal called for pos: {} getFilePointer: {} length: {}", pos, getFilePointer(), length());
        reBufferAt(pos);
        //seek(pos);
    }

    @Override
    public void seek(long newPosition) throws IOException
    {
        logger.debug("PageAlignedReader#seek() {} currentSubSegmentOffset: {} currentSubSegmentAlignedEndOffset: {}", newPosition, currentSubSegmentOffset, currentSubSegmentAlignedEndOffset);
        assert newPosition >= 0 && newPosition >= currentSubSegmentOffset && newPosition <= currentSubSegmentAlignedEndOffset;

        if (newPosition < 0)
            throw new IllegalArgumentException("new position should not be negative");

        if (buffer == null)
            logger.error("trying to do a seek on a reader that is already closed.. was closed originally by the following stack", closeException);

        if (buffer == null)
            throw new IllegalStateException("Attempted to seek in a closed RAR");

        long bufferOffset = bufferHolder.offset();
        if (newPosition >= bufferOffset && newPosition < bufferOffset + buffer.limit())
        {
            buffer.position((int) (newPosition - bufferOffset));
            return;
        }

        if (newPosition > length())
            throw new IllegalArgumentException(String.format("Unable to seek to position %d in %s (%d bytes) in read-only mode",
                                                             newPosition, getPath(), length()));
        reBufferAt(newPosition);
    }

    public void seekToStartOfCurrentSubSegment() throws IOException
    {
        seek(currentSubSegmentOffset);
    }

    public void seekToEndOfCurrentSubSegment() throws IOException
    {
        seek(currentSubSegmentAlignedEndOffset);
    }

    public void seekToEndOfCurrentSegment() throws IOException
    {
        seek(currentSegmentAlignedEndOffset);
    }

    public int getCurrentSegmentIdx()
    {
        return currentSegmentIdx;
    }

    public short getCurrentSubSegmentIdx()
    {
        return currentSubSegmentIdx;
    }

    public boolean isCurrentSubSegmentPageAligned()
    {
        return currentSubSegmentIsPageAligned;
    }

    public long getCurrentSubSegmentAlignedEndOffset()
    {
        return currentSubSegmentAlignedEndOffset;
    }

    public int getPageAlignedChunkSize()
    {
        return pageAlignedChunkSize;
    }

    public DataPosition mark()
    {
        return new PageAlignedFileMark(getOffset(), currentSegmentIdx, currentSubSegmentIdx);
    }

    public void reset(DataPosition mark) throws IOException
    {
        PageAlignedFileMark alignedMark = (PageAlignedFileMark) mark;
        if (!isPositionInsideCurrentSubSegment(alignedMark.pointer))
            setCurrentBoundsForSegment(alignedMark.currentSegmentIdx, alignedMark.currentSubSegmentIdx);
        seek(alignedMark.pointer);
    }

    protected long current()
    {
        return bufferHolder.offset() + buffer.position();
    }

    public long bytesPastMark(DataPosition mark)
    {
        assert mark instanceof PageAlignedFileMark;
        long bytes = current() - ((PageAlignedFileMark) mark).pointer;
        assert bytes >= 0;
        return bytes;
        //return 0;
    }

    public long getFilePointer()
    {
        return getOffset();
    }

    /**
     * Read length bytes from current file position
     *
     * @param length length of the bytes to read
     * @return buffer with bytes read
     * @throws IOException if any I/O operation failed
     */
    public ByteBuffer readBytes(int length) throws IOException
    {
        byte[] buf = new byte[length];
        try
        {
            int bytesRead = read(buf);
            assert bytesRead == length;
        }
        catch (BufferUnderflowException | BufferOverflowException e)
        {
            logger.error("Something bad happened reading from the current mmapped buffer with length {}.. current state of this PageAlignedReader: [{}]", length, toString(), e);
            throw e;
        }
        return ByteBuffer.wrap(buf);
    }

    public Exception getCloseException()
    {
        return closeException;
    }

    @Override
    public void close() throws IOException
    {
        logger.debug("PageAligednReader#close()... isClosed? {}", isClosed);
        if (isClosed)
            return;

        // tmp kjkj: capture stack when close() is called so
        // when a caller incorrectly operates on this same instance after
        // it has already been closed we can know who closed it originally..
        try
        {
            throw new Exception();
        }
        catch (Exception e)
        {
            closeException = e;
        }

        isClosed = true;

        bufferHolder.release();
        rebufferer.closeReader();
        buffer = null;
        bufferHolder = null;
    }

    public boolean isClosed()
    {
        return isClosed;
    }

    /**
     * Read data from file starting from current currentOffset to populate buffer.
     */
    public void reBuffer() throws IOException
    {
        if (isEOF())
            return;

        reBufferAt(getOffset());
    }

    private void reBufferAt(long position)
    {
        bufferHolder.release();
        bufferHolder = rebufferer.rebuffer(position);
        buffer = bufferHolder.buffer();
        buffer.position(Ints.checkedCast(position - bufferHolder.offset()));

        assert buffer.order() == ByteOrder.BIG_ENDIAN : "Buffer must have BIG ENDIAN byte ordering";
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        long offsetAfterSkip = getOffset() + n;
        assert offsetAfterSkip <= currentSegmentAlignedEndOffset;
        seek(offsetAfterSkip);
        return n;
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        int ch1 = readByte() & 0xFF;
        int ch2 = readByte() & 0xFF;
        if ((ch1 | ch2) < 0)
            throw new IOException("Failed to read unsigned short as deserialized value is bogus/negative");
        return (ch1 << 8) + (ch2);
    }

    public ChannelProxy getChannel()
    {
        return rebufferer.channel();
    }

    // A wrapper of the RandomAccessReader that closes the channel when done.
    // For performance reasons RAR does not increase the reference count of
    // a channel but assumes the owner will keep it open and close it,
    // see CASSANDRA-9379, this thin class is just for those cases where we do
    // not have a shared channel.
    static class RandomAccessReaderWithOwnChannel extends PageAlignedReader
    {
        RandomAccessReaderWithOwnChannel(Rebufferer rebufferer, boolean skipSetup) throws IOException
        {
            super(rebufferer, skipSetup);
        }

        @Override
        public void close() throws IOException
        {
            try
            {
                super.close();
            }
            finally
            {
                try
                {
                    rebufferer.close();
                }
                finally
                {
                    getChannel().close();
                }
            }
        }
    }

    /**
     * Open a PageAlignedReader (not compressed, not mmapped, no read throttling) that will own its channel.
     *
     * @param file File to open for reading
     * @return new RandomAccessReader that owns the channel opened in this method.
     * @throws IOException when we failed to properly deserialize the Descriptor component or properly setup and open the file
     */
    @SuppressWarnings("resource")
    public static PageAlignedReader open(File file) throws IOException
    {
        return open(file, false);
    }

    public static PageAlignedReader open(File file, boolean skipSetup) throws IOException
    {
        ChannelProxy channel = new ChannelProxy(file);
        try
        {
            ChunkReader reader = new SimpleChunkReader(channel, -1, BufferType.OFF_HEAP, DEFAULT_BUFFER_SIZE);
            Rebufferer rebufferer = reader.instantiateRebufferer();
            return new RandomAccessReaderWithOwnChannel(rebufferer, skipSetup);
        }
        catch (Throwable t)
        {
            channel.close();
            throw t;
        }
    }
}
