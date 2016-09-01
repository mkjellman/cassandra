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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.db.index.birch.AlignedSegment;
import org.apache.cassandra.db.index.birch.PageAlignedReader;

public class PageAlignedAwareSegmentedFile extends SegmentedFile
{
    private final File file;

    public PageAlignedAwareSegmentedFile(String path)
    {
        super(new Cleanup(path), path, new File(path).length());
        this.file = new File(path);
    }

    public PageAlignedAwareSegmentedFile sharedCopy()
    {
        return new PageAlignedAwareSegmentedFile(file.getAbsolutePath());
    }

    private static final class Cleanup extends SegmentedFile.Cleanup
    {
        private Cleanup(String path)
        {
            super(path);
        }

        public void tidy()
        {
            // todo "Try forcing the unmapping of segments using undocumented unsafe sun APIs."
            // see "MmappedSegmentedFile"
        }
    }

    public static class Builder extends SegmentedFile.Builder
    {
        public Builder()
        {
            super();
        }

        public void addPotentialBoundary(long boundary)
        {

        }

        protected SegmentedFile complete(String path, long overrideLength, boolean isFinal)
        {
            return new PageAlignedAwareSegmentedFile(path);
        }
    }

    public FileDataInput getSegment(long position)
    {
        try
        {
            PageAlignedReader reader = new PageAlignedReader(file);
            int segmentRes = reader.findIdxForPosition(position);
            reader.setSegment(segmentRes);
            return reader;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
