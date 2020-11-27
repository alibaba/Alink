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

package com.alibaba.alink.common.io.filesystem.copy.csv;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.Path;

import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.copy.FileOutputFormat;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

/**
 * A {@link FileOutputFormat} that writes objects to a text file.
 *
 * <p>Objects are converted to Strings using either {@link Object#toString()} or a {@link TextFormatter}.
 *
 * @param <T> type of elements
 */
@PublicEvolving
public class TextOutputFormat<T> extends FileOutputFormat <T> {

	private static final long serialVersionUID = 1L;

	private static final int NEWLINE = '\n';

	private String charsetName;

	private transient Charset charset;

	private String rowDelimiter;
	// --------------------------------------------------------------------------------------------

	/**
	 * Formatter that transforms values into their {@link String} representations.
	 *
	 * @param <IN> type of input elements
	 */
	public interface TextFormatter<IN> extends Serializable {
		String format(IN value);
	}

	public TextOutputFormat(Path outputPath, BaseFileSystem <?> fs, String rowDelimiter) {
		this(outputPath, "UTF-8", fs, rowDelimiter);
	}

	public TextOutputFormat(Path outputPath, String charset, BaseFileSystem <?> fs, String rowDelimiter) {
		super(outputPath, fs);
		this.charsetName = charset;
		this.rowDelimiter = rowDelimiter;
	}

	public String getCharsetName() {
		return charsetName;
	}

	public void setCharsetName(String charsetName) throws IllegalCharsetNameException, UnsupportedCharsetException {
		if (charsetName == null) {
			throw new NullPointerException();
		}

		if (!Charset.isSupported(charsetName)) {
			throw new UnsupportedCharsetException("The charset " + charsetName + " is not supported.");
		}

		this.charsetName = charsetName;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);

		try {
			this.charset = Charset.forName(charsetName);
		} catch (IllegalCharsetNameException e) {
			throw new IOException("The charset " + charsetName + " is not valid.", e);
		} catch (UnsupportedCharsetException e) {
			throw new IOException("The charset " + charsetName + " is not supported.", e);
		}
	}

	@Override
	public void writeRecord(T record) throws IOException {
		byte[] bytes = record.toString().getBytes(charset);
		this.stream.write(bytes);
		this.stream.write(
			rowDelimiter == null ?
				Character.valueOf((char) NEWLINE).toString().getBytes(charset) :
				rowDelimiter.getBytes(charset)
		);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return "TextOutputFormat (" + getOutputFilePath() + ") - " + this.charsetName;
	}
}
