package com.alibaba.alink.operator.common.io.reader;

import org.apache.flink.core.io.InputSplit;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.operator.common.io.csv.CsvFileInputSplit;
import com.alibaba.alink.operator.common.io.csv.CsvInputFormatBeta;
import com.alibaba.alink.operator.common.io.csv.CsvInputFormatBeta.CsvSplitInputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Readers for reading part of a http file.
 */
public class HttpFileSplitReader implements FileSplitReader, AutoCloseable {

	private static final long serialVersionUID = 510714179228030029L;
	private String path;
	private CsvInputFormatBeta inputFormat = null;
	private transient CsvFileInputSplit split;
	private transient HttpURLConnection connection;
	private transient InputStream stream;

	public HttpFileSplitReader(String path) {
		this.path = path;
	}

	public void open(InputSplit split, long start, long end) throws IOException {
		assert start >= 0;
		URL url = new URL(path);
		this.connection = (HttpURLConnection) url.openConnection();
		this.connection.setDoInput(true);
		this.connection.setConnectTimeout(5000);
		this.connection.setReadTimeout(60000);
		this.connection.setRequestMethod("GET");
		this.connection.setRequestProperty("Range", String.format("bytes=%d-%d", start, end));
		this.connection.connect();
		this.stream = this.connection.getInputStream();
		this.split = (CsvFileInputSplit) split;
	}

	@Override
	public void open(InputSplit split) throws IOException {
		long start = ((CsvFileInputSplit) split).start;
		long end = ((CsvFileInputSplit) split).end - 1;
		this.open(split, start, end);
	}

	@Override
	public void reopen(InputSplit split, long start) throws IOException {
		long end = ((CsvFileInputSplit) split).end - 1;
		this.open(split, start, end);
	}

	@Override
	public void close() throws IOException {
		if (this.stream != null) {
			this.stream.close();
		}
		if (this.connection != null) {
			this.connection.disconnect();
		}
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return this.stream.read(b, off, len);
	}

	@Override
	public long getFileLength() {
		HttpURLConnection headerConnection = null;
		try {
			URL url = new URL(path);
			headerConnection = (HttpURLConnection) url.openConnection();
			headerConnection.setConnectTimeout(5000);
			headerConnection.setRequestMethod("HEAD");

			headerConnection.connect();
			long contentLength = headerConnection.getContentLengthLong();
			String acceptRanges = headerConnection.getHeaderField("Accept-Ranges");
			boolean splitable = acceptRanges != null && acceptRanges.equalsIgnoreCase("bytes");

			if (contentLength < 0) {
				throw new AkIllegalDataException("The content length can't be determined because content length < 0.");
			}

			// If the http server does not accept ranges, then we quit the program.
			// This is because 'accept ranges' is required to achieve robustness (through re-connection),
			// and efficiency (through concurrent read).
			if (!splitable) {
				throw new AkIllegalDataException("Http-Header doesn't have header 'Accept-Ranges' or the value of "
					+ "'Accept-Ranges' value not equal 'bytes', The http server does not support range reading.");
			}

			return contentLength;
		} catch (Exception e) {
			throw new AkIllegalDataException(String.format("Fail to connect to http address %s", path), e);
		} finally {
			if (headerConnection != null) {
				headerConnection.disconnect();
			}
		}
	}

	@Override
	public long getSplitStart() {
		return split.start;
	}

	@Override
	public long getSplitEnd() {
		return split.end;
	}

	@Override
	public long getSplitNumber() {
		return split.getSplitNumber();
	}

	@Override
	public long getSplitLength() {
		return split.length;
	}

	@Override
	public CsvInputFormatBeta getInputFormat(String lineDelim, boolean ignoreFirstLine,
											 Character quoteChar) {
		if (inputFormat == null) {
			inputFormat = new CsvInputFormatBeta(this, lineDelim, ignoreFirstLine, quoteChar);
		}
		return inputFormat;
	}

	@Override
	public CsvInputFormatBeta convertFileSplitToInputFormat(String lineDelim, boolean ignoreFirstLine,
															Character quoteChar) {
		return new CsvSplitInputFormat(this, lineDelim, ignoreFirstLine, quoteChar);
	}

	@Override
	public InputSplit convertStringToSplitObject(String splitStr) {
		return CsvFileInputSplit.fromString(splitStr);
	}
}
