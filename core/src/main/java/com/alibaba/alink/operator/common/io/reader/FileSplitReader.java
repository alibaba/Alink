package com.alibaba.alink.operator.common.io.reader;

import com.alibaba.alink.operator.common.io.csv.CsvFileInputSplit;

import java.io.IOException;
import java.io.Serializable;

/**
 * Reader for a split of a file.
 */
public interface FileSplitReader extends Serializable {

    /**
     * Open for reading data range [start, end]
     */
    void open(CsvFileInputSplit split, long start, long end) throws IOException;

    /**
     * Close the reader.
     */
    void close() throws IOException;

    /**
     * Reads up to <code>len</code> bytes of data from the reader into
     * an array of bytes.  An attempt is made to read as many as
     * <code>len</code> bytes, but a smaller number may be read.
     * The number of bytes actually read is returned as an integer.
     * <p>
     * <p> This method blocks until input data is available, end of file is
     * detected, or an exception is thrown.
     * <p>
     * <p> If <code>len</code> is zero, then no bytes are read and
     * <code>0</code> is returned; otherwise, there is an attempt to read at
     * least one byte. If no byte is available because the stream is at end of
     * file, the value <code>-1</code> is returned; otherwise, at least one
     * byte is read and stored into <code>b</code>.
     */
    int read(byte b[], int off, int len) throws IOException;

    /**
     * Get length of the file.
     *
     * @return The length.
     */
    long getFileLength();
}
