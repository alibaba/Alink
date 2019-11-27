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

package com.alibaba.alink.operator.common.io.csv;

import org.apache.flink.core.io.InputSplit;

/**
 * The InputSplit for reading CSV files.
 */
public class CsvFileInputSplit implements InputSplit {

    /**
     * Starting position of this split.
     */
    public long start;

    /**
     * Length of this split.
     */
    public long length;

    /**
     * The ending position of this split.
     */
    public long end;

    private int numSplits;
    private int splitNo;

    /**
     * The constructor.
     *
     * @param numSplits     Number of splits.
     * @param splitNo       No. of this split.
     * @param contentLength Total length of the file in bytes.
     */
    public CsvFileInputSplit(int numSplits, int splitNo, long contentLength) {
        this.numSplits = numSplits;
        this.splitNo = splitNo;

        long avg = contentLength / numSplits;
        long remain = contentLength % numSplits;
        this.length = avg + (splitNo < remain ? 1 : 0);
        this.start = splitNo * avg + Long.min(remain, splitNo);
        long BUFFER_SIZE = 1024L * 1024L;
        this.end = Long.min(start + length + BUFFER_SIZE, contentLength);
    }

    @Override
    public String toString() {
        return "split: " + splitNo + "/" + numSplits + ", " + start + " " + length + " " + end;
    }

    @Override
    public int getSplitNumber() {
        return this.splitNo;
    }
}
