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

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.apache.flink.types.parser.FieldParser;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkParseErrorException;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.operator.common.io.dummy.DummyFiledParser;
import com.alibaba.alink.operator.common.io.reader.FileSplitReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Generic csv input format which supports multiple types of data reader.
 */
public class GenericCsvInputFormatBeta<T extends InputSplit> implements InputFormat <Row, T> {
	// -------------------------------------- Constants -------------------------------------------

	private static final Logger LOG = LoggerFactory.getLogger(GenericCsvInputFormatBeta.class);

	private static final long serialVersionUID = -7327585548493815210L;

	private static final int LINE_LENGTH_LIMIT = 1024 * 1024 * 1024;

	protected static final long READ_WHOLE_SPLIT_FLAG = -1L;

	protected final FileSplitReader reader;

	// The charset used to convert strings to bytes
	private String charsetName = "UTF-8";

	// Charset is not serializable
	private transient Charset charset;

	/**
	 * The default read buffer size = 1MB.
	 */
	private static final int BUFFER_SIZE = 1024 * 1024;

	// --------------------------------------------------------------------------------------------
	//  Variables for internal parsing.
	//  They are all transient, because we do not want them so be serialized
	//  They are copied from FileInputFormat,java
	// --------------------------------------------------------------------------------------------

	/**
	 * The start of the split that this parallel instance must consume.
	 */
	protected transient long splitStart;

	/**
	 * remaining bytes of current split to read
	 */
	private transient long splitLength;

	/**
	 * The current split that this parallel instance must consume.
	 */
	protected transient T currentSplit;

	// --------------------------------------------------------------------------------------------
	//  The configuration parameters. Configured on the instance and serialized to be shipped.
	// --------------------------------------------------------------------------------------------

	// The delimiter may be set with a byte-sequence or a String. In the latter
	// case the byte representation is updated consistent with current charset.
	private byte[] delimiter;

	// To speed up readRecord processing. Used to find windows line endings.
	// It is set when open so that readRecord does not have to evaluate it
	protected boolean lineDelimiterIsLinebreak = false;

	protected final boolean ignoreFirstLine;

	/**
	 * Some file input formats are not splittable on a block level (deflate)
	 * Therefore, the FileInputFormat can only read whole files.
	 * The file is unsplittable when quotedStringParsing is true.
	 */
	protected boolean unsplittable = false;
	protected boolean quotedStringParsing = false;

	protected byte quoteCharacter;

	// --------------------------------------------------------------------------------------------
	//  Transient variables copied from DelimitedInputFormat.java
	// --------------------------------------------------------------------------------------------

	private transient byte[] readBuffer; // buffer for holding data read by reader

	private transient byte[] wrapBuffer;

	private transient int readPos;       // reading position of the read buffer

	private transient int limit;         // number of valid bytes in the read buffer

	private transient byte[] currBuffer;        // buffer in which current record byte sequence is found
	private transient int currOffset;           // offset in above buffer
	private transient int currLen;              // length of current byte sequence

	private transient boolean overLimit; // flag indicating whether we have read beyond the split

	private transient boolean end;

	private long offset = -1;

	private transient long bytesRead;    // number of bytes read by reader

	private transient boolean readerClosed;

	// for parsing fields of a reacord
	private transient FieldParser <?> fieldParser = null;
	private transient Object[] holders = null;
	private boolean fieldInQuote;

	// --------------------------------------------------------------------------------------------
	//  Constructors & Getters/setters for the configurable parameters
	// --------------------------------------------------------------------------------------------

	public GenericCsvInputFormatBeta(FileSplitReader reader, String delimiter, boolean ignoreFirstLine) {
		this.reader = reader;
		this.charset = Charset.forName(charsetName);
		this.delimiter = delimiter.getBytes();
		this.ignoreFirstLine = ignoreFirstLine;
	}

	public GenericCsvInputFormatBeta(FileSplitReader reader, String delimiter, boolean ignoreFirstLine,
									 Character quoteChar) {
		this(reader, delimiter, ignoreFirstLine);
		if (quoteChar != null) {
			this.unsplittable = true;
			this.quotedStringParsing = true;
			this.quoteCharacter = (byte) quoteChar.charValue();
		}
	}

	public GenericCsvInputFormatBeta(FileSplitReader reader, String delimiter, boolean ignoreFirstLine,
									 boolean unsplittable, Character quoteChar) {
		this(reader, delimiter, ignoreFirstLine);
		if (quoteChar != null) {
			this.unsplittable = unsplittable;
			this.quoteCharacter = (byte) quoteChar.charValue();
		}
		this.quotedStringParsing = true;
	}

	protected void setEnd(Boolean end) {this.end = end;}

	protected void setFieldInQuote(boolean fieldInQuote) {this.fieldInQuote = fieldInQuote;}

	/**
	 * Opens the given input split. This method opens the input stream to the specified file, allocates read buffers
	 * and positions the stream at the correct position, making sure that any partial record at the beginning is
	 * skipped.
	 *
	 * @param split The input split to open.
	 * @see org.apache.flink.api.common.io.FileInputFormat#open(org.apache.flink.core.fs.FileInputSplit)
	 */
	@Override
	public void open(T split) throws IOException {
		this.currentSplit = split;
		// open and assign a split to the reader
		this.reader.open(split);
		this.readerClosed = false;

		this.splitStart = this.reader.getSplitStart();
		this.splitLength = this.reader.getSplitLength();

		this.charset = Charset.forName(charsetName);

		this.bytesRead = 0L;

		initBuffers();

		initializeParsers();

		// left to right evaluation makes access [0] okay
		// this marker is used to fasten up readRecord, so that it doesn't have to check each call if the line ending
		// is set to default
		if (delimiter.length == 1 && delimiter[0] == '\n') {
			this.lineDelimiterIsLinebreak = true;
		}
	}

	// copied from DelimitedInputFormat.initBuffers
	private void initBuffers() {
		if (BUFFER_SIZE <= this.delimiter.length) {
			throw new AkIllegalDataException(String.format(
				"Buffer size is %d, and delimiter length is %d. Buffer size must be greater than length of delimiter.",
				BUFFER_SIZE, this.delimiter.length));
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

		// instantiate the parsers
		fieldParser = new DummyFiledParser();
		fieldParser.setCharset(charset);
		if (this.quotedStringParsing) {
			((DummyFiledParser) fieldParser).enableQuotedStringParsing(this.quoteCharacter);
		}
		this.holders = new Object[] {fieldParser.createValue()};
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
			if (0 == this.currLen) {
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
		boolean findQuote = this.fieldInQuote;

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
						setResult(this.wrapBuffer, 0, countInWrapBuffer,findQuote);
						return true;
					} else {
						return false;
					}
				}
			}

			int startPos = this.readPos - delimPos;
			int count;
			// Search for next occurence of delimiter in read buffer.
			while (this.readPos < this.limit && delimPos < this.delimiter.length) {
				if (!findQuote && (this.readBuffer[this.readPos]) == this.delimiter[delimPos]) {
					// Found the expected delimiter character. Continue looking for the next character of delimiter.
					delimPos++;
				} else if (quotedStringParsing && this.readBuffer[this.readPos] == quoteCharacter) {
					findQuote = !findQuote;
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
			if (delimPos == this.delimiter.length) {
				// we found a delimiter
				int readBufferBytesRead = this.readPos - startPos;
				count = readBufferBytesRead - this.delimiter.length;

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
					setResult(this.wrapBuffer, 0, countInWrapBuffer + count,findQuote);
					return true;
				} else {
					setResult(this.readBuffer, startPos, count,findQuote);
					return true;
				}
			} else {
				// we reached the end of the readBuffer
				count = this.limit - startPos;

				// check against the maximum record length
				if (((long) countInWrapBuffer) + count > LINE_LENGTH_LIMIT) {
					throw new AkIllegalDataException("The record length exceeded the maximum record length (" +
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

	private void setResult(byte[] buffer, int offset, int len, boolean fieldInQuote) {
		this.currBuffer = buffer;
		this.currOffset = offset;
		this.currLen = len;
		this.fieldInQuote = fieldInQuote;
	}

	/**
	 * Fills the read buffer with bytes read from the file starting from an offset.
	 * Returns false if has reached the end of the split and nothing is read.
	 */
	protected boolean fillBuffer(int offset) throws IOException {
		int maxReadLength = this.readBuffer.length - offset;
		// special case for reading the whole split.
		if (this.splitLength == READ_WHOLE_SPLIT_FLAG) {
			int read = this.reader.read(this.readBuffer, offset, maxReadLength);
			if (read == -1) {
				this.reader.close();
				this.readerClosed = true;
				return false;
			} else {
				this.readPos = offset;
				this.limit = read;
				return true;
			}
		}

		// else ..
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
		long start = this.reader.getSplitStart();
		long end = this.reader.getSplitEnd();

		while (this.bytesRead + start < end && read == -1 && tryTimes < maxTryTimes) {
			read = this.reader.read(this.readBuffer, offset, toRead);

			// unexpected EOF encountered, re-establish the connection
			if (read < 0) {
				this.reader.close();
				//this.reader.open(this.currentSplit);
				this.reader.reopen(this.currentSplit, splitStart + bytesRead);
			}
			tryTimes++;
		}

		if (tryTimes >= maxTryTimes) {
			throw new AkUnclassifiedErrorException("Fail to read data.");
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
	public T[] createInputSplits(int minNumSplits) throws IOException {
		return null;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(T[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	protected Row readRecord(Row reuse, byte[] bytes, int offset, int numBytes) throws IOException {
		Row reuseRow;
		if (reuse == null) {
			reuseRow = new Row(1);
		} else {
			reuseRow = reuse;
		}

		// Found window's end line, so find carriage return before the newline
		if (this.lineDelimiterIsLinebreak && numBytes > 0 && bytes[offset + numBytes - 1] == '\r') {
			//reduce the number of bytes so that the Carriage return is not taken as data
			numBytes--;
		}

		int startPos = offset;
		int field = 0;
		FieldParser <Object> parser = (FieldParser <Object>) fieldParser;

		int newStartPos = parser.resetErrorStateAndParse(
			bytes,
			startPos,
			offset + numBytes,
			delimiter,
			holders[field]);

		if (parser.getErrorState() != FieldParser.ParseErrorState.NONE) {
			 if (parser.getErrorState() == FieldParser.ParseErrorState.EMPTY_COLUMN) {
				reuseRow.setField(field, null);
			} else {
				throw new AkParseErrorException(
					String.format("Parsing error for column %1$s of row '%2$s' originated by %3$s: %4$s.",
						field, new String(bytes, offset, numBytes), parser.getClass().getSimpleName(),
						parser.getErrorState()));
			}
		} else {
			reuseRow.setField(field, parser.getLastResult());
		}

		if (newStartPos >= 0) {
			startPos = newStartPos;
		}

		return reuseRow;
	}

}
