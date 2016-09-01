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

import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.FileUtils;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads an aligned and segmented file. Documentation on the
 * the binary format used to serialize compatable files is available in the
 * {@link org.apache.cassandra.db.index.birch.PageAlignedWriter} docs.
 * <p>
 * Instances of this class are <b>not</b> thread safe.
 *
 * @see PageAlignedWriter
 */
public class PageAlignedReader implements FileDataInput, Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(PageAlignedReader.class);

    private final File file;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final MappedByteBuffer serializedSegmentDescriptorsBuf;
    private final int totalSegments;
    private final long maxOffset;

    private MappedByteBuffer currentMmappedBuffer = null;
    private SegmentBounds currentMmappedBufferBounds = new SegmentBounds(0, 0);
    private AlignedSegment currentSegment = null;
    private AlignedSubSegment currentSubSegment = null;

    public PageAlignedReader(File file) throws IOException
    {
        this.file = file;
        this.raf = new RandomAccessFile(file, "r");
        this.channel = raf.getChannel();
        raf.seek(raf.length() - Long.BYTES);
        long serializedDescriptorsStartingOffset = raf.readLong();
        long serializedSegmentDescriptorsLength = (raf.length() - Long.BYTES) - serializedDescriptorsStartingOffset;
        this.serializedSegmentDescriptorsBuf = channel.map(FileChannel.MapMode.READ_ONLY, serializedDescriptorsStartingOffset, serializedSegmentDescriptorsLength);
        this.totalSegments = this.serializedSegmentDescriptorsBuf.getInt();
        this.serializedSegmentDescriptorsBuf.getInt(); // relative starting offset of serialized segments
        this.maxOffset = this.serializedSegmentDescriptorsBuf.getLong();
        raf.seek(0);
    }

    protected long getNextAlignedOffset(long offset)
    {
        // if offset <= alignTo, return 0
        // if next aligned offset > offset, return aligned offset - alignTo
        int alignTo = currentSubSegment.pageChunkSize;
        if (alignTo == 0 || offset == alignTo)
            return offset;
        if (offset <= alignTo)
            return 0;
        long alignedOffset = (offset + alignTo - 1) & ~(alignTo - 1);
        return (alignedOffset > offset) ? alignedOffset - alignTo : alignedOffset;
    }

    /**
     * @param idx the index of the aligned segment to deserialize
     * @return the deserialized AlignedSegment
     * @throws IOException thrown if an IO Error occurs while deserializing the AlignedSegment
     */
    private AlignedSegment getAlignedSegmentAtIdx(int idx) throws IOException
    {
        serializedSegmentDescriptorsBuf.position(0);

        serializedSegmentDescriptorsBuf.getInt(); // total number of serialized aligned segments
        int serializedObjectsStartingOffset = serializedSegmentDescriptorsBuf.getInt();

        serializedSegmentDescriptorsBuf.position(Integer.BYTES + Integer.BYTES + Long.BYTES + (idx * (Long.BYTES + Integer.BYTES)));
        serializedSegmentDescriptorsBuf.getLong(); // starting offset of this idx's segment in the file
        int relativeOffsetToSerializedSegment = serializedSegmentDescriptorsBuf.getInt();
        serializedSegmentDescriptorsBuf.getLong(); // starting offset of idx+1's segment in the file
        int nextIdxRelativeOffsetToSerializedSegment = serializedSegmentDescriptorsBuf.getInt();
        // calculate the serialized size of the AlignedSegment by subtracting the start of idx + 1 from the start of idx
        int serializedLengthOfSegment = nextIdxRelativeOffsetToSerializedSegment - relativeOffsetToSerializedSegment;

        // move the position of the backing memory-mapped to the starting offset for this serialized segment
        serializedSegmentDescriptorsBuf.position(serializedObjectsStartingOffset + relativeOffsetToSerializedSegment);
        // read the calculated number of bytes (serializedLengthOfSegment) from the memory-mapped segment
        byte[] serializedSegmentBytes = new byte[serializedLengthOfSegment];
        serializedSegmentDescriptorsBuf.get(serializedSegmentBytes);
        // deserialize the AlignedSegment object and return it.
        AlignedSegment alignedSegment;
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serializedSegmentBytes))
        {
            alignedSegment = AlignedSegment.SERIALIZER.deserialize(new DataInputStream(bais));
        }

        return alignedSegment;
    }

    /**
     * @return total number of Aligned Segments encoded in this file
     */
    public int numberOfSegments()
    {
        return totalSegments;
    }

    /**
     * @return a new instance of AlignedSegmentIterator
     */
    public AlignedSegmentIterator getSegmentIterator()
    {
        return new AlignedSegmentIterator();
    }

    /**
     * Returns an Iterator that will return all AlignedSegments in this PageAligned
     * file. Each returned AlignedSegment is lazily constructed as needed.
     */
    public class AlignedSegmentIterator extends AbstractIterator<AlignedSegment>
    {
        int currentSegment = 0;

        @Override
        public AlignedSegment computeNext()
        {
            if (currentSegment + 1 > totalSegments)
            {
                return endOfData();
            }

            try
            {
                return getAlignedSegmentAtIdx(currentSegment++);
            }
            catch (IOException e)
            {
                logger.error("Failed to deserialize aligned segment [idx {}] from {}", currentSegment, file.getAbsolutePath());
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

        AlignedSegment segment = getAlignedSegmentAtIdx(segmentId);
        currentSegment = segment;
        currentSubSegment = segment.getSubSegments().get(subSegmentId);
        seek(currentSubSegment.offset);
        setCurrentMmappedBuffer();
    }

    public void nextSubSegment() throws IOException
    {
        setSegment(currentSegment.idx, currentSubSegment.idx + 1);
    }

    public boolean hasNextSubSegment()
    {
        return currentSubSegment.idx + 1 < currentSegment.getSubSegments().size();
    }

    public boolean hasNextSegment()
    {
        return currentSegment.idx + 1 < totalSegments;
    }

    public boolean hasPreviousSegment()
    {
        return currentSegment.idx - 1 >= 0;
    }

    public void previousSegment() throws IOException
    {
        setSegment(currentSegment.idx - 1);
    }

    public void nextSegment() throws IOException
    {
        setSegment(currentSegment.idx + 1);
    }

    private void setCurrentMmappedBuffer() throws IOException
    {
        if (currentSubSegment.shouldUseSingleMmappedBuffer())
        {
            assert currentSubSegment.alignedLength < ((1 << 21) - 1); //2GB max mmap-able size
            maybeFreeCurrentMmappedBuffer();
            currentMmappedBuffer = channel.map(FileChannel.MapMode.READ_ONLY, raf.getFilePointer(), currentSubSegment.length);
            currentMmappedBufferBounds = new SegmentBounds(currentSubSegment.offset, currentSubSegment.offset + currentSubSegment.alignedLength - 1);
        }
        else
        {
            long lengthToMap = (raf.length() - raf.getFilePointer() < currentSubSegment.pageChunkSize) ? raf.length() - raf.getFilePointer() : currentSubSegment.pageChunkSize;
            maybeFreeCurrentMmappedBuffer();
            currentMmappedBuffer = channel.map(FileChannel.MapMode.READ_ONLY, raf.getFilePointer(), lengthToMap);
            currentMmappedBufferBounds = new SegmentBounds(raf.getFilePointer(), raf.getFilePointer() + (currentSubSegment.pageChunkSize - 1));
        }
    }

    public MappedByteBuffer getMmappedBuffer(long fileOffset, int length) throws IOException
    {
        return channel.map(FileChannel.MapMode.READ_ONLY, fileOffset, length);
    }

    protected int getCurrentInternalBufferPosition()
    {
        return currentMmappedBuffer.position();
    }

    public long getOffset() throws IOException
    {
        return raf.getFilePointer() + currentMmappedBuffer.position();
    }

    public String getPath()
    {
        return file.getAbsolutePath();
    }

    public boolean isEOF() throws IOException
    {
        boolean isOutOfSegments = currentSegment.idx + 1 >= totalSegments;
        boolean isOutOfSubSegments = currentSubSegment.idx + 1 >= currentSegment.getSubSegments().size();
        boolean currentSubSegmentExausted = raf.getFilePointer() + currentMmappedBuffer.position() >= currentSubSegment.getEndOffset();
        return isOutOfSegments && isOutOfSubSegments && currentSubSegmentExausted;
    }

    public boolean isCurrentSegmentExausted() throws IOException
    {
        return raf.getFilePointer() + currentMmappedBuffer.position() >= currentSubSegment.getEndOffset();
    }

    public long bytesRemaining() throws IOException
    {
        return currentSegment.length - currentMmappedBuffer.position();
    }

    /**
     * Scans thru all segments and subsegments and does a bounds check for the provided position.
     * When a segment + sub segment combination is found that contains the given position, the
     * pointers for both the current segment and subsegment are updated to that segment.
     *
     * Every effort should be made to avoid this check as it's obviously not optimal, but if
     * PageAlignedReader instances are reused, it might be required that we use it to re-prepare
     * the PageAlignedReader instance. A good example of where this is required is in
     * {@link SSTableScanner#seekToCurrentRangeStart()}
     *
     * @param pos absolute position in the data file to find the segment and subsegment bounds for
     * @throws IOException thrown if an exception is encountered while mapping the found region
     */
    public void findAndSetSegmentAndSubSegmentCurrentForPosition(long pos) throws IOException
    {
        assert pos >= 0 && pos <= maxOffset;

        if (pos < currentSegment.offset || pos > currentSegment.endOffset)
        {
            int segmentIdxContainingPos = findIdxForPosition(pos);
            AlignedSegment segmentForPos = getAlignedSegmentAtIdx(segmentIdxContainingPos);
            currentSegment = segmentForPos;
            currentSubSegment = segmentForPos.getSubSegments().get(0);
        }

        if (pos > currentSubSegment.getEndOffset() || pos < currentSubSegment.offset && (pos >= currentSegment.offset && pos <= currentSegment.endOffset))
        {
            for (int i = currentSubSegment.idx + 1; i < currentSegment.getSubSegments().size(); i++)
            {
                AlignedSubSegment subSegment = currentSegment.getSubSegments().get(i);
                if (pos >= subSegment.offset && pos <= subSegment.getEndOffset())
                {
                    currentSubSegment = subSegment;
                    break;
                }
            }
        }

        setCurrentMmappedBuffer();
        seek(pos);
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

        serializedSegmentDescriptorsBuf.position(0);

        int entries = serializedSegmentDescriptorsBuf.getInt();
        int serializedObjectsStartingOffset = serializedSegmentDescriptorsBuf.getInt();
        long endFileOffsetOfLastSegment = serializedSegmentDescriptorsBuf.getLong();

        long ret = -1;
        int retIdx = -1;

        int start = 0;
        int end = entries - 1; // binary search is zero-indexed
        int middle = (end - start) / 2;

        while (start <= end)
        {
            serializedSegmentDescriptorsBuf.position((middle * (Long.BYTES + Integer.BYTES)) + Integer.BYTES + Integer.BYTES + Long.BYTES);

            long offset = serializedSegmentDescriptorsBuf.getLong();
            int offsetToSerializedSegment = serializedSegmentDescriptorsBuf.getInt();

            int cmp = Long.compare(offset, pos);
            if (cmp == 0)
            {
                return middle;
            }

            if (cmp < 0)
            {
                ret = offset;
                retIdx = middle;
                start = middle + 1;
            }
            else
            {
                end = middle - 1;
            }
            middle = (start + end) / 2;
        }

        return retIdx;
    }

    public void seek(long pos) throws IOException
    {
        // the position provided to seek should be within the bounds of the current segment/subSegment.
        // this ensures that we don't need to check if we need to randomly re-mmap a region of the file
        // from a different segment (which is expensive as the position is not guarenteed to be at the
        // start or end of the segment, so the only way to determine the segment is to iterate thru all
        // segment and subsegments until a bounds check matches the requested position). So, to avoid
        // this, callers are responsible for updating the current segment/subSegment to one that
        // contains the resired position, prior to calling seek.
        assert pos >= 0 && pos >= currentSubSegment.offset && pos <= currentSubSegment.getEndOffset();

        long alignedSegmentOffset = getNextAlignedOffset(pos);
        if (currentMmappedBuffer != null && currentMmappedBufferBounds.offsetWithinBounds(pos))
        {
            int relativeOffset = (int) (pos - alignedSegmentOffset);
            currentMmappedBuffer.position(relativeOffset);
        }
        else
        {
            channel.position(alignedSegmentOffset);
            setCurrentMmappedBuffer();
            int relativeOffset = (int) (pos - alignedSegmentOffset);
            currentMmappedBuffer.position(relativeOffset);
        }
    }

    public AlignedSegment getCurrentSegment()
    {
        return currentSegment;
    }

    public AlignedSubSegment getCurrentSubSegment()
    {
        return currentSubSegment;
    }

    public FileMark mark()
    {
        try
        {
            return new PageAlignedFileMark(raf.getFilePointer(), currentMmappedBuffer.position());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void reset(FileMark mark) throws IOException
    {
        PageAlignedFileMark alignedMark = (PageAlignedFileMark) mark;
        channel.position(alignedMark.rafPointer);
        findAndSetSegmentAndSubSegmentCurrentForPosition(alignedMark.getSyntheticPointer());
        seek(alignedMark.getSyntheticPointer());
    }

    public long bytesPastMark(FileMark mark)
    {
        return 0;
    }

    public long getFilePointer()
    {
        try
        {
            return channel.position();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
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
        currentMmappedBuffer.get(buf);
        return ByteBuffer.wrap(buf);
    }

    @Override
    public void close() throws IOException
    {
        /*
         * Try forcing the unmapping of segments using undocumented unsafe sun APIs.
         * If this fails (non Sun JVM), we'll have to wait for the GC to finalize the mapping.
         * If this works and a thread tries to access any segment, hell will unleash on earth.
         */
        maybeFreeCurrentMmappedBuffer();

        if (FileUtils.isCleanerAvailable())
        {
            if (serializedSegmentDescriptorsBuf != null)
                FileUtils.clean(serializedSegmentDescriptorsBuf);
        }

        channel.close();
        raf.close();
    }

    private void maybeFreeCurrentMmappedBuffer()
    {
        if (FileUtils.isCleanerAvailable())
        {
            if (currentMmappedBuffer != null)
            {
                FileUtils.clean(currentMmappedBuffer);
                currentMmappedBuffer = null;
            }
        }
    }

    @Override
    public void readFully(byte[] b) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        long offsetAfterSkip = raf.getFilePointer() + n;
        assert offsetAfterSkip <= currentSegment.endOffset;
        seek(offsetAfterSkip);
        setCurrentMmappedBuffer();
        return n;
    }

    @Override
    public boolean readBoolean() throws IOException
    {
        return ((int) currentMmappedBuffer.get()) == 1;
    }

    @Override
    public byte readByte() throws IOException
    {
        return currentMmappedBuffer.get();
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public short readShort() throws IOException
    {
        return currentMmappedBuffer.getShort();
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        int ch1 = currentMmappedBuffer.get();
        int ch2 = currentMmappedBuffer.get();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (ch1 << 8) + (ch2 << 0);
    }

    @Override
    public char readChar() throws IOException
    {
        return currentMmappedBuffer.getChar();
    }

    @Override
    public int readInt() throws IOException
    {
        return currentMmappedBuffer.getInt();
    }

    @Override
    public long readLong() throws IOException
    {
        return currentMmappedBuffer.getLong();
    }

    @Override
    public float readFloat() throws IOException
    {
        return currentMmappedBuffer.getFloat();
    }

    @Override
    public double readDouble() throws IOException
    {
        return currentMmappedBuffer.getDouble();
    }

    @Override
    public String readLine() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        try
        {
            return String.format("totalSegments: %s raf.getFilePointer(): %s currentMmappedBuffer.position(): " +
                                 "%s currentSegment: %s currentSubSegment: %s", totalSegments,
                                 raf.getFilePointer(),
                                 (currentMmappedBuffer == null) ? -1 : currentMmappedBuffer.position(),
                                 (currentSegment == null) ? "null" : currentSegment.toString(),
                                 (currentSubSegment == null) ? "null" : currentSubSegment.toString());
        }
        catch (IOException e)
        {
            return String.format("Failed to stringify PageAlignedReader because %s", e.getCause().getMessage());
        }
    }

    public static class SegmentBounds
    {
        public final long startOffset;
        public final long endOffset;

        public SegmentBounds(long startOffset, long endOffset)
        {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        public boolean offsetWithinBounds(long offset)
        {
            return offset >= startOffset && offset <= endOffset;
        }
    }
}
