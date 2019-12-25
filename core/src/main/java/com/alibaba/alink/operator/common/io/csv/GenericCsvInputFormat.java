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

import com.alibaba.alink.operator.common.io.reader.FileSplitReader;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Generic csv input format which supports multiple types of data reader.
 */
public class GenericCsvInputFormat implements InputFormat<Row, CsvFileInputSplit> {

    private static final int LINE_LENGTH_LIMIT = 1024 * 1024;
    private static final int BUFFER_SIZE = 1024 * 1024;
    private final boolean ignoreFirstLine;
    private FileSplitReader reader;
    private String charsetName = "UTF-8";
    private String fieldDelimStr;
    private TypeInformation<?>[] fieldTypes;
    // The delimiter may be set with a byte-sequence or a String. In the latter
    // case the byte representation is updated consistent with current charset.
    private byte[] fieldDelim;
    private byte[] lineDelim;

    // --------------------------------------------------------------------------------------------
    //  Variables for internal parsing.
    //  They are all transient, because we do not want them so be serialized
    // --------------------------------------------------------------------------------------------
    // for reading records
    private transient Charset charset;

    private transient long splitLength;  // remaining bytes of current split to read
    private transient byte[] readBuffer; // buffer for holding data read by reader
    private transient long bytesRead;    // number of bytes read by reader
    private transient boolean overLimit; // flag indicating whether we have read beyond the split
    private transient int limit;         // number of valid bytes in the read buffer
    private transient int readPos;       // reading position of the read buffer

    private transient byte[] wrapBuffer;
    private transient byte[] currBuffer;        // buffer in which current record byte sequence is found
    private transient int currOffset;           // offset in above buffer
    private transient int currLen;              // length of current byte sequence

    private transient boolean readerClosed;
    private transient boolean end;

    private transient CsvFileInputSplit split;

    // for parsing fields of a record
    private transient FieldParser<?>[] fieldParsers = null;
    private transient Object[] holders = null;

    public GenericCsvInputFormat(FileSplitReader reader, TypeInformation<?>[] fieldTypes,
                                 String fieldDelim, String lineDelim, boolean ignoreFirstLine) {
        this.reader = reader;
        this.fieldTypes = fieldTypes;
        this.charset = Charset.forName(charsetName);
        this.fieldDelim = fieldDelim.getBytes(charset);
        this.fieldDelimStr = fieldDelim;
        this.lineDelim = lineDelim.getBytes();
        this.ignoreFirstLine = ignoreFirstLine;
    }

    private static Class<?>[] extractTypeClasses(TypeInformation[] fieldTypes) {
        Class<?>[] classes = new Class<?>[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            classes[i] = fieldTypes[i].getTypeClass();
        }
        return classes;
    }

    private void initBuffers() {
        if (BUFFER_SIZE <= this.lineDelim.length) {
            throw new IllegalArgumentException("Buffer size must be greater than length of delimiter.");
        }

        if (this.readBuffer == null || this.readBuffer.length != BUFFER_SIZE) {
            this.readBuffer = new byte[BUFFER_SIZE];
        }
        if (this.wrapBuffer == null || this.wrapBuffer.length < 256) {
            this.wrapBuffer = new byte[256];
        }

        this.readPos = 0;
        this.limit = 0;
        this.overLimit = false;
        this.end = false;
    }

    private void initializeParsers() {
        Class<?>[] fieldClasses = extractTypeClasses(fieldTypes);

        // instantiate the parsers
        FieldParser<?>[] parsers = new FieldParser<?>[fieldClasses.length];

        for (int i = 0; i < fieldClasses.length; i++) {
            if (fieldClasses[i] != null) {
                Class<? extends FieldParser<?>> parserType = FieldParser.getParserForType(fieldClasses[i]);
                if (parserType == null) {
                    throw new RuntimeException("No parser available for type '" + fieldClasses[i].getName() + "'.");
                }

                FieldParser<?> p = InstantiationUtil.instantiate(parserType, FieldParser.class);
                p.setCharset(charset);
                parsers[i] = p;
            }
        }
        this.fieldParsers = parsers;
        this.holders = new Object[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            holders[i] = fieldParsers[i].createValue();
        }
    }

    /**
     * Opens the given input split. This method opens the input stream to the specified file, allocates read buffers
     * and positions the stream at the correct position, making sure that any partial record at the beginning is
     * skipped.
     *
     * @param split The input split to open.
     * @see org.apache.flink.api.common.io.FileInputFormat#open(org.apache.flink.core.fs.FileInputSplit)
     */
    @Override
    public void open(CsvFileInputSplit split) throws IOException {
        this.charset = Charset.forName(charsetName);
        this.splitLength = split.length;
        this.split = split;
        this.bytesRead = 0L;

        initBuffers();
        this.reader.open(split, split.start, split.end - 1);
        this.readerClosed = false;
        initializeParsers();

        if (split.getSplitNumber() > 0 || ignoreFirstLine) {
            readLine();
            // if the first partial record already pushes the stream over
            // the limit of our split, then no record starts within this split
            if (this.overLimit) {
                this.end = true;
            }
        } else {
            fillBuffer(0);
        }
    }

    /**
     * Closes the input by releasing all buffers and closing the file input stream.
     *
     * @throws IOException Thrown, if the closing of the file stream causes an I/O error.
     */
    @Override
    public void close() throws IOException {
        this.wrapBuffer = null;
        this.readBuffer = null;
        if (!this.readerClosed) {
            this.reader.close();
            this.readerClosed = true;
        }
    }

    /**
     * Checks whether the current split is at its end.
     *
     * @return True, if the split is at its end, false otherwise.
     */
    @Override
    public boolean reachedEnd() {
        return this.end;
    }

    @Override
    public Row nextRecord(Row record) throws IOException {
        if (readLine()) {
            if (0 == this.currLen && fieldTypes.length > 1) {
                // current line is empty, then skip
                return nextRecord(record);
            } else {
                return readRecord(record, this.currBuffer, this.currOffset, this.currLen);
            }
        } else {
            this.end = true;
            return null;
        }
    }

    /**
     * Copy one line of data from "readBuffer" to "currBuffer".
     * If "readBuffer" is fully consumed, trigger "fillBuffer()" to fill it.
     */
    protected final boolean readLine() throws IOException {
        if (this.readerClosed || this.overLimit) {
            return false;
        }

        int countInWrapBuffer = 0;

        // position of matching positions in the delimiter byte array
        int delimPos = 0;

        while (true) {
            if (this.readPos >= this.limit) {
                // readBuffer is completely consumed. Fill it again but keep partially read delimiter bytes.
                if (!fillBuffer(delimPos)) {
                    int countInReadBuffer = delimPos;
                    if (countInWrapBuffer + countInReadBuffer > 0) {
                        // we have bytes left to emit
                        if (countInReadBuffer > 0) {
                            // we have bytes left in the readBuffer. Move them into the wrapBuffer
                            if (this.wrapBuffer.length - countInWrapBuffer < countInReadBuffer) {
                                // reallocate
                                byte[] tmp = new byte[countInWrapBuffer + countInReadBuffer];
                                System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
                                this.wrapBuffer = tmp;
                            }

                            // copy readBuffer bytes to wrapBuffer
                            System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer,
                                countInReadBuffer);
                            countInWrapBuffer += countInReadBuffer;
                        }
                        setResult(this.wrapBuffer, 0, countInWrapBuffer);
                        return true;
                    } else {
                        return false;
                    }
                }
            }

            int startPos = this.readPos - delimPos;
            int count;

            // Search for next occurence of delimiter in read buffer.
            while (this.readPos < this.limit && delimPos < this.lineDelim.length) {
                if ((this.readBuffer[this.readPos]) == this.lineDelim[delimPos]) {
                    // Found the expected delimiter character. Continue looking for the next character of delimiter.
                    delimPos++;
                } else {
                    // Delimiter does not match.
                    // We have to reset the read position to the character after the first matching character
                    //   and search for the whole delimiter again.
                    readPos -= delimPos;
                    delimPos = 0;
                }
                readPos++;
            }

            // check why we dropped out
            if (delimPos == this.lineDelim.length) {
                // we found a delimiter
                int readBufferBytesRead = this.readPos - startPos;
                count = readBufferBytesRead - this.lineDelim.length;

                // copy to byte array
                if (countInWrapBuffer > 0) {
                    // check wrap buffer size
                    if (this.wrapBuffer.length < countInWrapBuffer + count) {
                        final byte[] nb = new byte[countInWrapBuffer + count];
                        System.arraycopy(this.wrapBuffer, 0, nb, 0, countInWrapBuffer);
                        this.wrapBuffer = nb;
                    }
                    if (count >= 0) {
                        System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, count);
                    }
                    setResult(this.wrapBuffer, 0, countInWrapBuffer + count);
                    return true;
                } else {
                    setResult(this.readBuffer, startPos, count);
                    return true;
                }
            } else {
                // we reached the end of the readBuffer
                count = this.limit - startPos;

                // check against the maximum record length
                if (((long) countInWrapBuffer) + count > LINE_LENGTH_LIMIT) {
                    throw new IOException("The record length exceeded the maximum record length (" +
                        LINE_LENGTH_LIMIT + ").");
                }

                // Compute number of bytes to move to wrapBuffer
                // Chars of partially read delimiter must remain in the readBuffer. We might need to go back.
                int bytesToMove = count - delimPos;
                // ensure wrapBuffer is large enough
                if (this.wrapBuffer.length - countInWrapBuffer < bytesToMove) {
                    // reallocate
                    byte[] tmp = new byte[Math.max(this.wrapBuffer.length * 2, countInWrapBuffer + bytesToMove)];
                    System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
                    this.wrapBuffer = tmp;
                }

                // copy readBuffer to wrapBuffer (except delimiter chars)
                System.arraycopy(this.readBuffer, startPos, this.wrapBuffer, countInWrapBuffer, bytesToMove);
                countInWrapBuffer += bytesToMove;
                // move delimiter chars to the beginning of the readBuffer
                System.arraycopy(this.readBuffer, this.readPos - delimPos, this.readBuffer, 0, delimPos);

            }
        }
    }

    private void setResult(byte[] buffer, int offset, int len) {
        this.currBuffer = buffer;
        this.currOffset = offset;
        this.currLen = len;
    }

    /**
     * Fills the read buffer with bytes read from the file starting from an offset.
     * Returns false if has reached the end of the split and nothing is read.
     */
    private boolean fillBuffer(int offset) throws IOException {
        int maxReadLength = this.readBuffer.length - offset;
        int toRead;
        if (this.splitLength > 0) {
            // if we have some data to read in the split, read that
            toRead = this.splitLength > maxReadLength ? maxReadLength : (int) this.splitLength;
        } else {
            // if we have exhausted our split, we need to complete the current record, or read one
            // more across the next split.
            // the reason is that the next split will skip over the beginning until it finds the first
            // delimiter, discarding it as an incomplete chunk of data that belongs to the last record in the
            // previous split.
            toRead = maxReadLength;
            this.overLimit = true;
        }

        int tryTimes = 0;
        int maxTryTimes = 10;
        int read = -1;
        while (this.bytesRead + this.split.start < this.split.end && read == -1 && tryTimes < maxTryTimes) {
            read = this.reader.read(this.readBuffer, offset, toRead);

            // unexpected EOF encountered, re-establish the connection
            if (read < 0) {
                this.reader.close();
                System.out.println("reconnecting ...");
                this.reader.open(this.split, this.split.start + bytesRead, this.split.end - 1);
            }
            tryTimes++;
        }

        if (tryTimes >= maxTryTimes) {
            throw new RuntimeException("Fail to read data.");
        }

        if (read == -1) {
            this.reader.close();
            this.readerClosed = true;
            return false;
        } else {
            this.splitLength -= read;
            this.readPos = offset; // position from where to start reading
            this.limit = read + offset; // number of valid bytes in the read buffer
            this.bytesRead += read;
            return true;
        }
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return null;
    }

    @Override
    public CsvFileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        CsvFileInputSplit[] splits;
        splits = new CsvFileInputSplit[minNumSplits];
        long contentLength = reader.getFileLength();
        for (int i = 0; i < splits.length; i++) {
            splits[i] = new CsvFileInputSplit(minNumSplits, i, contentLength);
        }
        return splits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(CsvFileInputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    protected Row readRecord(Row reuse, byte[] bytes, int offset, int numBytes) throws IOException {
        Row reuseRow;
        if (reuse == null) {
            reuseRow = new Row(fieldTypes.length);
        } else {
            reuseRow = reuse;
        }

        // Found window's end line, so find carriage return before the newline
        if (numBytes > 0 && bytes[offset + numBytes - 1] == '\r') {
            //reduce the number of bytes so that the Carriage return is not taken as data
            numBytes--;
        }

        int startPos = offset;
        for (int field = 0; field < fieldTypes.length; field++) {
            FieldParser<Object> parser = (FieldParser<Object>) fieldParsers[field];

            int newStartPos = parser.resetErrorStateAndParse(
                bytes,
                startPos,
                offset + numBytes,
                fieldDelim,
                holders[field]);

            if (parser.getErrorState() != FieldParser.ParseErrorState.NONE) {
                if (parser.getErrorState() == FieldParser.ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR) {
                    reuseRow.setField(field, null);
                } else if (parser.getErrorState() == FieldParser.ParseErrorState.NUMERIC_VALUE_ILLEGAL_CHARACTER) {
                    reuseRow.setField(field, null);
                } else if (parser.getErrorState() == FieldParser.ParseErrorState.EMPTY_COLUMN) {
                    reuseRow.setField(field, null);
                } else {
                    throw new ParseException(
                        String.format("Parsing error for column %1$s of row '%2$s' originated by %3$s: %4$s.",
                            field, new String(bytes, offset, numBytes), parser.getClass().getSimpleName(),
                            parser.getErrorState()));
                }
            } else {
                reuseRow.setField(field, parser.getLastResult());
            }

            if (newStartPos < 0) {
                if (field < fieldTypes.length - 1) { // skip next field delimiter
                    while (startPos + fieldDelim.length <= offset + numBytes && (!FieldParser.delimiterNext(bytes,
                        startPos, fieldDelim))) {
                        startPos++;
                    }
                    if (startPos + fieldDelim.length > offset + numBytes) {
                        throw new RuntimeException("Can't find next field delimiter: " + "\"" + fieldDelimStr + "\","
                            + " " +
                            "Perhaps the data is invalid or do not match the schema." +
                            "The row is: " + new String(bytes, offset, numBytes));
                    }
                    startPos += fieldDelim.length;
                }
            } else {
                startPos = newStartPos;
            }
        }

        return reuseRow;
    }

}
