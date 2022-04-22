package com.alibaba.alink.common.dl.exchange;

import com.alibaba.flink.ml.util.SpscOffHeapQueue;

import java.io.Closeable;
import java.io.IOException;

/**
 * A data bridge exchanges bytes between processes without dependency of `MLContext`.
 */
public class BytesDataExchange implements Closeable {
	private final static int DEFAULT_QUEUE_SIZE = 8 * 1024 * 1024;

	private final BytesRecordWriter writer;
	private final BytesRecordReader reader;
	private long numReadRecords = 0;
	private long numWriteRecords = 0;

	private final SpscOffHeapQueue readQueue;
	private final SpscOffHeapQueue writeQueue;

	public BytesDataExchange(String inQueueFileName, String outQueueFileName) throws Exception {
		this(inQueueFileName, outQueueFileName, DEFAULT_QUEUE_SIZE);
	}

	public BytesDataExchange(String inQueueFileName, String outQueueFileName, int queueSize) throws Exception {
		readQueue = new SpscOffHeapQueue(inQueueFileName, queueSize);
		writeQueue = new SpscOffHeapQueue(outQueueFileName, queueSize);

		writer = new BytesRecordWriter(new SpscOffHeapQueue.QueueOutputStream(writeQueue));
		reader = new BytesRecordReader(new SpscOffHeapQueue.QueueInputStream(readQueue));
	}

	public <T> boolean write(byte[] bytes) throws IOException {
		numWriteRecords++;
		return writer.write(bytes);
	}

	public byte[] read(boolean readWait) throws IOException {
		byte[] res;
		if (readWait) {
			res = reader.read();
		} else {
			res = reader.tryRead();
		}
		if (null == res) {
			return null;
		} else {
			numReadRecords++;
			return res;
		}
	}

	public long getNumReadRecords() {
		return numReadRecords;
	}

	public long getNumWriteRecords() {
		return numWriteRecords;
	}

	public void markWriteFinished() {
		writeQueue.markFinished();
	}

	public void close() throws IOException {
		reader.close();
		writer.close();

		readQueue.close();
		writeQueue.close();
	}
}
