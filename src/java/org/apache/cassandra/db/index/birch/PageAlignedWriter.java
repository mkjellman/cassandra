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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputStreamAndChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * <strong>1.1. Page Aligned File Format</strong>
 * <pre>
 * {@code
 *                  1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 3 3 3 3 3 3 3 3
 *    1 2 3 5 6 7 8 0 1 2 4 5 6 7 9 0 1 3 4 5 6 8 9 0 2 3 4 5 7 8 9
 *    2 5 8 1 4 6 9 2 5 8 0 3 6 9 2 4 7 0 3 6 8 1 4 7 0 2 5 8 1 4 6
 *  0 8 6 4 2 0 8 6 4 2 0 8 6 4 2 0 8 6 4 2 0 8 6 4 2 0 8 6 4 2 0 8
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                   Segment 1.1               ||    S1 Padding  |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                        Segment 2.1                            /
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * /            Segment 2.1 (cont.)         ||      S2.1 Padding   |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                                                               |
 * +                                                               +
 * |      Segment 2.2 (segment internally 4096 aligned/padded)     |
 * +                                                               +
 * |                                                               |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |    Segment Pointers   |p|
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+
 * }
 * </pre>
 * <p>
 * <strong>1.2. 'Segment Pointers' Serialized Format</strong>
 * <pre>
 * {@code
 *            1 1 1 1 1 2 2 2 2 2 3 3 3 3 3 4 4 4 4 4 5 5 5 5 5 6 6
 *  0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |       Number of Elements      |  Initial Serialization Offset |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |              Final Segment Max Valid File Offset              |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                  Segment Start Offset (s1)                    |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |Relative Serialization Pos (s1)|   Segment Start Offset (s2)   /
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |Segment Start Offset cont. (s2)|Relative Serialization Pos (s2)|
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                  Segment Start Offset (s3)                    |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |Relative Serialization Pos (s3)|    Segment End Offset (s3)    /
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | Segment End Offset cont. (s3) |Rel. Serialization End Pos (s3)||
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                                                               /
 * /                     Serialized Segments                       /
 * /                                                               |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |    File Offset to First Byte of Serialized Segments (this)    |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * }
 * </pre>
 *
 * PageAlignedWriter serializes out aligned segmented sections. It
 * enables deserializable-friendly serialization of data structures
 * that are both internally page-chunked/aligned and padded and non
 * padded structures into the same file.
 * <p>
 * For instance, this enables the encoding of many independent key and a tree
 * structures, while enforcing that the segment is encoded and starts
 * on a configurable page boundary (4096 by default).
 * <p>
 * In Cassandra we only encode a index for a key if the row exceeds the value
 * returned by {@link org.apache.cassandra.config.Config#column_index_size_in_kb},
 * which defaults to 64kb. If the row does not exceed column_index_size_in_kb,
 * only the offset for the start of the row in the data component of the SSTable
 * is encoded.
 * <p>
 * This means we must support serializing multiple keys in a single file back-to-back
 * without indexes followed by a entry for a key with an index. To cleanly support this,
 * PageAlignedWriter implements the concepts of {@link org.apache.cassandra.db.index.birch.AlignedSubSegment}
 * and {@link org.apache.cassandra.db.index.birch.AlignedSegment}. A segment can have
 * n-number of sub-segments. Each sub-segment will be aligned to the closest
 * boundary, and the final segment itself will be aligned.
 * <p>
 * When all segments have been serialied, the file is finalized by encoding
 * offsets for the segments (and their subsegments) at the end of the file,
 * followed by a single long with the offset of the start of these encoded
 * segment pointers. This makes deserialization simple: seek to the end of the
 * file minus Long.BYTES. Read a long, which get's you the starting offset of
 * the encoded segment pointers. These pointers then contain the offsets
 * to each segment (and the padding/alignment requirements that segment
 * was serialized with).
 * <p>
 * The Segment Pointers section is serialized in a way to optimize deserialization.
 * Originally, the segments were simply serialized one after another. On deserialization,
 * the PageAlignedReader constructor would deserialize all AlignedSegments. This is
 * highly inefficient for three main reasons: 1) there is a high CPU cost while
 * deserializing the segments. During this time the thread will be blocked. 2) if
 * a given SSTable contains a very large number of keys, as there is a 1:1 mapping
 * of keys to segments, this will cause a significant number of objects to be allocated
 * on the heap. 3) if a PageAlignedReader is created and intended to be used for only
 * one key, we don't want to deserialize all segments just to throw all - 1 of them away.
 * <p>
 * To address these concerns/lessons and optimize for deserialization/reading, the number of segments,
 * relative offset to the variable length actual serialized segment bytes, end offset of the final segment,
 * and the starting file offset [long] and relative offset inside the segment pointers section to the full
 * serialized AlignedSegment for each segment to be serialized. We encode the starting file offset and
 * serialized relative position for the number of elements + 1, where the +1 is actually the end offset
 * of the final object. This allows calculation of the serialized size of the final object.
 * <p>
 * By serializing the fixed elements at the front, this allows us to find a matching segment for a
 * given position by binary searching over the fixed starting offsets on disk. Once we find the
 * required segment, we can skip to the offset and deserialize the entire AlignedSegment object
 * when necessary. This means we only allocate objects if 100% necessary to service a request.
 * <p>
 * Instances of this class are <b>*not*</b> thread safe.
 *
 * @see PageAlignedReader
 * @see org.apache.cassandra.io.util.PageAlignedAwareSegmentedFile
 * @see AlignedSegment
 * @see AlignedSubSegment
 */
public class PageAlignedWriter implements WritableByteChannel
{
    private static final Logger logger = LoggerFactory.getLogger(PageAlignedWriter.class);

    public static final int DEFAULT_PAGE_ALIGNMENT_BOUNDARY = 4096;
    public static final int SEGMENT_NOT_PAGE_ALIGNED = 0;

    public final DataOutputPlus stream; // stream is eventually flushed and written to out
    private final RandomAccessFile out; // out contains the aligned data written to stream
    private final String filePath;

    protected Runnable runPostFlush;
    protected long lastFlushOffset;

    private long currentSegmentStartOffset;
    private long currentSubSegmentStartOffset;
    private int currentSubSegmentPageBlockSize;
    private boolean segmentInProgress = false;
    private boolean subSegmentInProgress = false;

    private List<AlignedSubSegment> finishedSubSegments;

    private final List<AlignedSegment> finishedSegments;

    public PageAlignedWriter(File file)
    {
        try
        {
            out = new RandomAccessFile(file, "rw");
            filePath = file.getAbsolutePath();
            stream = new DataOutputStreamAndChannel(this);
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }

        this.finishedSegments = new ArrayList<>();
    }

    public static PageAlignedWriter open(File file)
    {
        return new PageAlignedWriter(file);
    }

    public void seek(long pos) throws IOException
    {
        out.seek(pos);
    }

    /**
     * Aligns the current open file being written to to the next aligned
     * page boundary. This method will only align the file to the next aligned
     * offset if the file isn't already on an aligned offset.
     *
     * For instance if the current file offset is 24 and alignFile() is called,
     * the file pointer will be moved from 24 to the next boundary (4096 by default).
     *
     * Then some bytes are written and alignFile() is called twice from two seperate
     * methods (but without anything being written into the file), this will cause the
     * file to be aligned first from 16384 => 16384 (for example), and if called again,
     * from 16384 => 16384 (or a no-op).
     *
     * @throws IOException thrown if seek(pos) on the underlying file threw an exception
     */
    private void alignFile() throws IOException
    {
        long curPos = out.getFilePointer();
        long alignedOffset = getNextAlignedOffset(curPos);
        out.seek(alignedOffset);
    }

    public void startNewSegment() throws IOException
    {
        assert !segmentInProgress;

        this.currentSegmentStartOffset = out.getFilePointer();
        this.finishedSubSegments = new ArrayList<>();
        this.segmentInProgress = true;
    }

    public void startNewNonPageAlignedSubSegment() throws IOException
    {
        assert segmentInProgress && !subSegmentInProgress;

        this.currentSubSegmentPageBlockSize = SEGMENT_NOT_PAGE_ALIGNED;
        this.currentSubSegmentStartOffset = out.getFilePointer();
        this.subSegmentInProgress = true;
    }

    public void startNewSubSegment(int pageBlockSize) throws IOException
    {
        assert segmentInProgress && !subSegmentInProgress;
        // ensure page block size is a valid multiple of 2
        assert ((pageBlockSize & (pageBlockSize & (pageBlockSize - 1))) == 0);

        this.currentSubSegmentPageBlockSize = pageBlockSize;
        this.currentSubSegmentStartOffset = out.getFilePointer();
        this.subSegmentInProgress = true;
    }

    public void finalizeCurrentSegment() throws IOException
    {
        assert segmentInProgress && !subSegmentInProgress;

        long endOffset = finishedSubSegments.get(finishedSubSegments.size() - 1).getEndOffset();

        alignFile();

        long alignedEndOffset = out.getFilePointer();
        long segmentLength = endOffset - currentSegmentStartOffset;
        long alignedSegmentLength = alignedEndOffset - currentSegmentStartOffset;

        // todo...
        // assert segmentLength > MIN PAGE SIZE (yes kjellman ???)
        // assert finishing/ending on page boundary?

        finishedSegments.add(new AlignedSegment(finishedSegments.size(), currentSegmentStartOffset, segmentLength,
                                                alignedSegmentLength, finishedSubSegments));

        this.finishedSubSegments = null;
        this.segmentInProgress = false;
    }

    public void finalizeCurrentSubSegment() throws IOException
    {
        assert segmentInProgress && subSegmentInProgress;

        long endOffset = out.getFilePointer();

        alignFile();

        long alignedEndOffset = out.getFilePointer();
        long subSegmentLength = endOffset - currentSubSegmentStartOffset;
        long alignedSubSegmentLength = alignedEndOffset - currentSubSegmentStartOffset;

        // sanity check to make sure we aren't writing out corrupt data as we
        // should never have the contents of subSegment be greater than the max
        // size of the subSegment (which is the length of the alligned offset
        // for this given subSegment)
        assert subSegmentLength <= alignedSubSegmentLength;

        finishedSubSegments.add(new AlignedSubSegment(finishedSubSegments.size(), currentSubSegmentStartOffset, subSegmentLength,
                                                      alignedSubSegmentLength, currentSubSegmentPageBlockSize));

        subSegmentInProgress = false;
    }

    private int alignTo()
    {
        return (currentSubSegmentPageBlockSize == 0) ? DEFAULT_PAGE_ALIGNMENT_BOUNDARY : currentSubSegmentPageBlockSize;
    }

    public long getNextAlignedOffset(long offset)
    {
        // if we are writing a non-aligned segment, we still want to make sure
        // we align the end to the next page alignment boundary so the next segment
        // starts at an aligned page boundary
        int alignTo = alignTo();
        return (offset + alignTo - 1) & ~(alignTo - 1);
    }

    private void serializeSegmentPointers() throws IOException
    {
        long segmentPointersStartingOffset = out.getFilePointer();

        // make sure starting offset of segments is aligned on a good boundary..
        assert ((segmentPointersStartingOffset % alignTo()) == 0);

        // first, serialize the total number of segments we're going to serialize
        out.writeInt(finishedSegments.size());

        // calculate the "relative" starting offset where we'll serialize the full variable
        // length segments
        int serializedSegmentsStartingOffset = Integer.BYTES + Integer.BYTES + Long.BYTES + ((Long.BYTES + Integer.BYTES) * (finishedSegments.size() + 1));
        out.writeInt(serializedSegmentsStartingOffset);

        // write out the maximum aligned end offset of the final segment.
        // this allows us to guard against bogus reader requests on deserialization
        // without needing to deserialize the entire final segment first to get this
        // value
        out.writeLong(finishedSegments.get(finishedSegments.size() - 1).alignedEndOffset);

        int currentRelativeOffsetToSerializedSegments = 0;
        long maxOffsetWritten = 0;

        int currentSegmentIdx = 0;
        for (AlignedSegment segment : finishedSegments)
        {
            // for each segment, skip forwards by the fixed overhead (number of elements,
            // relative starting offset the segments will be serialized at, max segment offset)
            // and the size of the fixed components times the index of the segment we're serializing
            out.seek(segmentPointersStartingOffset + Integer.BYTES + Integer.BYTES + Long.BYTES + (segment.idx * (Long.BYTES + Integer.BYTES)));

            // the starting file offset for this segment
            out.writeLong(segment.offset);
            // the relative offset to start reading at to deserialize the AlignedSegment object
            out.writeInt(currentRelativeOffsetToSerializedSegments);

            // get the file position after writing the fixed components.
            // after we skip to the position to seriaize the actual AlignedSegment
            // object and serialize it, we'll return the position back to this offset
            long currentFilePos = out.getFilePointer();
            long currentAbsoluteSerializedSegmentsOffset = segmentPointersStartingOffset + serializedSegmentsStartingOffset
                                                           + currentRelativeOffsetToSerializedSegments;
            out.seek(segmentPointersStartingOffset + serializedSegmentsStartingOffset + currentRelativeOffsetToSerializedSegments);
            AlignedSegment.SERIALIZER.serialize(segment, stream);
            maxOffsetWritten = out.getFilePointer();
            // figure out how many bytes we ended up serializing for this AlignedSegment.
            // we add this to our current relative offset so the next time thru we'll encode
            // the next element at the correct new offset
            long bytesSerialized = out.getFilePointer() - currentAbsoluteSerializedSegmentsOffset;
            currentRelativeOffsetToSerializedSegments += bytesSerialized;

            out.seek(currentFilePos);

            // if we're on the final segment, we also serialize the end offset.
            // this allows us to calculate the size to deserialize when reading
            // by determining the size of the final element by calculating the
            // last element + 1 - last element
            if (currentSegmentIdx == finishedSegments.size() - 1)
            {
                out.writeLong(maxOffsetWritten);
                out.writeInt(currentRelativeOffsetToSerializedSegments);
            }
            currentSegmentIdx++;
        }

        out.seek(maxOffsetWritten);

        // the *very* last thing we write is the pointer to the
        // segment pointers.. this way we can look at the end of
        // the file minus 1 long to find the encoded segment pointers
        out.writeLong(segmentPointersStartingOffset);
    }

    public PageAlignedFileMark mark() throws IOException
    {
        return new PageAlignedFileMark(out.getFilePointer());
    }

    public void reset(PageAlignedFileMark mark) throws IOException
    {
        out.seek(mark.rafPointer);
    }

    public long getFilePointer() throws IOException
    {
        return out.getFilePointer();
    }

    public String getPath()
    {
        return filePath;
    }

    public boolean currentPageHasSpace(int sizeToWrite) throws IOException
    {
        long remainingSpaceInPage = out.getFilePointer() % currentSubSegmentPageBlockSize;
        return remainingSpaceInPage + sizeToWrite < currentSubSegmentPageBlockSize;
    }

    public long getCurrentFilePosition() throws IOException
    {
        return out.getFilePointer();
    }

    public void writeShort(int v) throws IOException
    {
        assert v < 1 << 15;
        out.writeShort(v);
    }

    public void writeShort(short v) throws IOException
    {
        out.writeShort(v);
    }

    public void writeInt(int v) throws IOException
    {
        out.writeInt(v);
    }

    public void writeLong(long v) throws IOException
    {
        out.writeLong(v);
    }

    public void writeByte(byte v) throws IOException
    {
        out.writeByte(v);
    }

    public void writeByte(int v) throws IOException
    {
        assert v < ((1 << 7) - 1);
        out.writeByte(v);
    }

    public void writeBoolean(boolean b) throws IOException
    {
        out.writeByte(b ? 1 : 0);
    }

    public void write(byte[] b, int off, int len) throws IOException
    {
        out.write(b, off, len);
    }

    public void setPostFlushListener(Runnable runPostFlush)
    {
        assert this.runPostFlush == null;
        this.runPostFlush = runPostFlush;
    }

    public long getLastFlushOffset()
    {
        return lastFlushOffset;
    }

    public void finishAndFlush()
    {
        if (runPostFlush != null)
            runPostFlush.run();
    }

    public int write(ByteBuffer src, int off, int len) throws IOException
    {
        // todo: use a shared buffer like SequentialWriter..
        byte[] buf = new byte[len];
        ByteBufferUtil.arrayCopy(src, off, buf, 0, len);
        out.write(buf);
        return len;
    }

    @Override
    public int write(ByteBuffer src) throws IOException
    {
        // todo: use a shared buffer like SequentialWriter..
        byte[] buf = new byte[src.remaining()];
        src.get(buf);
        out.write(buf);
        return buf.length;
    }

    @Override
    public boolean isOpen()
    {
        return out.getChannel().isOpen();
    }

    @Override
    public void close()
    {
        assert !segmentInProgress;

        try
        {
            // todo: is it right to do this here? or should serializeSegmentPointers do it automagically always?
            long alignedOffset = getNextAlignedOffset(out.getFilePointer());
            out.seek(alignedOffset);

            serializeSegmentPointers();
            out.getFD().sync();
            out.close();

            finishAndFlush();
        }
        catch (IOException e)
        {
            logger.error("Failed to serialize segment pointers", e);
            throw new RuntimeException(e);
        }
    }
}
