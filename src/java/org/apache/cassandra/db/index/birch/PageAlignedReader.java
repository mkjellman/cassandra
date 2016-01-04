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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads an aligned and segmented file. Documentation on the
 * the binary format used to serialize compatable files is available in the
 * {@link org.apache.cassandra.db.index.birch.PageAlignedWriter} docs.
 * <p>
 * Instances of this class are <b>not</b> thread safe.
 *
 * @see PageAlignedWriter
 */
public class PageAlignedReader implements FileDataInput
{
    private final File file;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final List<AlignedSegment> segments;
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
        this.segments = deserializeSegments();
        this.maxOffset = segments.get(segments.size() - 1).alignedEndOffset;
    }

    public static long getNextAlignedOffset(long offset)
    {
        return offset & 0xfffffffffffff000L;
    }

    private List<AlignedSegment> deserializeSegments() throws IOException
    {
        List<AlignedSegment> segments = new ArrayList<>();

        raf.seek(raf.length() - Long.BYTES);
        raf.seek(raf.readLong());

        int numSegments = raf.readInt();

        for (int i = 0; i < numSegments; i++)
        {
            AlignedSegment deserializedSegment = AlignedSegment.SERIALIZER.deserialize(raf);
            segments.add(deserializedSegment);
        }

        return segments;
    }

    public List<AlignedSegment> getSegments()
    {
        return segments;
    }

    public void setSegment(int segmentId) throws IOException
    {
        setSegment(segmentId, 0);
    }

    public void setSegment(int segmentId, int subSegmentId) throws IOException
    {
        assert segmentId < segments.size();

        AlignedSegment segment = segments.get(segmentId);
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
        return currentSegment.idx + 1 < segments.size();
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

    public int getSegmentForOffset(long offset) throws IOException
    {
        int segmentIdx = -1;
        for (int i = 0; i < segments.size(); i++)
        {
            if (offset >= segments.get(i).offset && offset < segments.get(i).endOffset)
            {
                segmentIdx = i;
                break;
            }
        }

        if (segmentIdx == -1) {
            throw new IOException(String.format("Provided offset %d is outside the bounds of all known segments", offset));
        }

        return segmentIdx;
    }

    private void setCurrentMmappedBuffer() throws IOException
    {
        if (currentSubSegment.shouldUseSingleMmappedBuffer())
        {
            assert currentSubSegment.alignedLength < ((1 << 21) - 1); //2GB max mmap-able size
            currentMmappedBuffer = channel.map(FileChannel.MapMode.READ_ONLY, raf.getFilePointer(), currentSubSegment.length);
            currentMmappedBufferBounds = new SegmentBounds(currentSubSegment.offset, currentSubSegment.offset + currentSubSegment.alignedLength - 1);
        }
        else
        {
            currentMmappedBuffer = channel.map(FileChannel.MapMode.READ_ONLY, raf.getFilePointer(), currentSubSegment.pageChunkSize);
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
        boolean isOutOfSegments = currentSegment.idx + 1 >= segments.size();
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
            for (AlignedSegment segment : segments)
            {
                if (pos >= segment.offset && pos <= segment.endOffset)
                {
                    currentSegment = segment;
                    currentSubSegment = segment.getSubSegments().get(0);
                    break;
                }
            }
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

        if (currentMmappedBuffer != null && currentMmappedBufferBounds.offsetWithinBounds(pos))
        {
            int relativeOffset = (int) (pos - getNextAlignedOffset(pos));
            currentMmappedBuffer.position(relativeOffset);
        }
        else
        {
            long alignedSegmentOffset = getNextAlignedOffset(pos);
            if (alignedSegmentOffset > pos || pos == currentSubSegment.getEndOffset())
                alignedSegmentOffset -= currentSubSegment.pageChunkSize;
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
        currentMmappedBuffer = null;
        channel.close();
        raf.close();
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
                                 "%s currentSegment: %s currentSubSegment: %s", (segments == null) ? -1 :segments.size(),
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
