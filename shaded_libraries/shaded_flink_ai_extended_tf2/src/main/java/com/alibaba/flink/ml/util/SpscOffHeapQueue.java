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

package com.alibaba.flink.ml.util;

import org.jctools.util.PortableJvmInfo;
import org.jctools.util.Pow2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.alibaba.flink.ml.util.SysUtil.UNSAFE;

/**
 * A ring buffer can be used as a queue between two threads or two processes (IPC). Modified from
 * http://psy-lob-saw.blogspot.com/2013/04/lock-free-ipc-queue.html
 * <p>
 * NOTE: this queue assumes: single producer (SP) and single consumer (SC)
 */
public final class SpscOffHeapQueue implements Closeable {
	private static Logger LOG = LoggerFactory.getLogger(SpscOffHeapQueue.class);

	private final long raw;
	private final long alignedRaw;

	// consumer owns read and writeCache
	private final long readAddress;
	private final long writeCacheAddress;
	private final long capacityAddress;

	// producer owns write and readCache
	private final long writeAddress;
	private final long readCacheAddress;

	private final long finishAddress;

	private final int capacity;
	private final int mask;
	private final long arrayBase;

	private MMapper mmapper;
	private String mmapFileName;

	private volatile boolean closed = false;

	@Override
	public String toString() {
		return "SpscOffHeapQueue{" +
			"raw=" + raw +
			", alignedRaw=" + alignedRaw +
			", readAddress=" + readAddress +
			", writeCacheAddress=" + writeCacheAddress +
			", capacityAddress=" + capacityAddress +
			", writeAddress=" + writeAddress +
			", readCacheAddress=" + readCacheAddress +
			", finishAddress=" + finishAddress +
			", capacity=" + capacity +
			", mask=" + mask +
			", arrayBase=" + arrayBase +
			", mmapFileName='" + mmapFileName + '\'' +
			", read=" + getRead() +
			", write=" + getWrite() +
			'}';
	}

    /* Memory layout:
    |read----|writeCach|-------|---------|--------|--------|--------|--------|
    |capacity|--------|--------|---------|--------|--------|--------|--------|
    |write---|readCach|--------|---------|--------|--------|--------|--------|
    |finish--|--------|--------|---------|--------|--------|--------|--------|
    |........|........|........|.........|........|........|........|........|
    |........|........|........|.........|........|........|........|........|
    * */

	/**
	 * This is to be used for an IPC queue with the direct buffer used being a memory mapped file.
	 */
	public SpscOffHeapQueue(final int capacity) {
		this(UNSAFE.allocateMemory(getRequiredBufferSize(capacity) + PortableJvmInfo.CACHE_LINE_SIZE),
			Pow2.roundToPowerOfTwo(capacity), true);
	}

	public SpscOffHeapQueue(final String mmapFileName, final int capacity) throws Exception {
		this(new MMapper(mmapFileName, getRequiredBufferSize(capacity) + PortableJvmInfo.CACHE_LINE_SIZE), capacity,
			true);
		this.mmapFileName = mmapFileName;
	}

	public SpscOffHeapQueue(final String mmapFileName, final int capacity, final boolean reset) throws Exception {
		this(new MMapper(mmapFileName, getRequiredBufferSize(capacity) + PortableJvmInfo.CACHE_LINE_SIZE), capacity,
			reset);
		this.mmapFileName = mmapFileName;
	}

	private SpscOffHeapQueue(final MMapper mmapper, final int capacity, final boolean reset) {
		this(mmapper.getAddress(), capacity, reset);
		this.mmapper = mmapper;
	}

	private SpscOffHeapQueue(final long buff, final int capacity, final boolean reset) {
		raw = buff;
		this.capacity = Pow2.roundToPowerOfTwo(capacity);
		mask = this.capacity - 1;
		alignedRaw = Pow2.align(buff, PortableJvmInfo.CACHE_LINE_SIZE);

		readAddress = alignedRaw;
		writeCacheAddress = readAddress + 8;
		capacityAddress = readAddress + PortableJvmInfo.CACHE_LINE_SIZE;
		writeAddress = readAddress + 2 * PortableJvmInfo.CACHE_LINE_SIZE;
		readCacheAddress = writeAddress + 8;
		finishAddress = writeAddress + PortableJvmInfo.CACHE_LINE_SIZE;
		arrayBase = alignedRaw + 4 * PortableJvmInfo.CACHE_LINE_SIZE;

		if (reset) {
			reset();
		}
	}

	public void reset() {
		UNSAFE.setMemory(alignedRaw, getRequiredBufferSize(capacity), (byte) 0);
		UNSAFE.putLongVolatile(null, capacityAddress, capacity);
	}

	/**
	 * Closes the mapper and frees memory. Using the queue after it's closed is likely to cause issues like SIGSEGV.
	 */
	@Override
	public synchronized void close() {
		closed = true;
		if (mmapper != null) {
			try {
				mmapper.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			UNSAFE.freeMemory(raw);
		}
	}

	public long getMmapLen() {
		if (mmapper == null) {
			return 0;
		}
		return mmapper.getSize();
	}

	private long getReadPlain() {
		return UNSAFE.getLong(null, readAddress);
	}

	private long getRead() {
		return UNSAFE.getLongVolatile(null, readAddress);
	}

	private void setRead(final long value) {
		UNSAFE.putOrderedLong(null, readAddress, value);
	}

	private long getWritePlain() {
		return UNSAFE.getLong(null, writeAddress);
	}

	private long getWrite() {
		return UNSAFE.getLongVolatile(null, writeAddress);
	}

	private void setWrite(final long value) {
		UNSAFE.putOrderedLong(null, writeAddress, value);
	}

	private long getReadCache() {
		return UNSAFE.getLong(null, readCacheAddress);
	}

	private void setReadCache(final long value) {
		UNSAFE.putLong(readCacheAddress, value);
	}

	private long getWriteCache() {
		return UNSAFE.getLong(null, writeCacheAddress);
	}

	private void setWriteCache(final long value) {
		UNSAFE.putLong(writeCacheAddress, value);
	}

	private boolean isFinished() {
		long v = UNSAFE.getLongVolatile(null, finishAddress);
		return v != 0;
	}

	public synchronized void markFinished() {
		if (!closed) {
			UNSAFE.putOrderedLong(null, finishAddress, -1);
		}
	}

	public long getArrayBase() {
		return arrayBase;
	}

	public int getMask() {
		return mask;
	}

	public int getCapacity() {
		return capacity;
	}

	public long getRawBuf() {
		return raw;
	}

	private static int getRequiredBufferSize(final int capacity) {
		return 4 * PortableJvmInfo.CACHE_LINE_SIZE + (Pow2.roundToPowerOfTwo(capacity));
	}

	public static long nextWrap(long v, int capacity) {
		int mask = capacity - 1;
		if ((v & mask) == 0) {
			return v + capacity;
		}
		return Pow2.align(v, capacity);
	}

	private static long allocateAlignedByteBuffer(int capacity, int align) {
		if (Long.bitCount(align) != 1) {
			throw new IllegalArgumentException("Alignment must be a power of 2");
		}
		long address = UNSAFE.allocateMemory(capacity + align);
		return Pow2.align(address, align);
	}

	/**
	 * The input stream based on this queue
	 */
	public static class QueueInputStream extends InputStream {
		private SpscOffHeapQueue queue;
		private byte[] readBuf = new byte[10 * 1024];
		ByteBuffer wrapped = ByteBuffer.wrap(readBuf, 0, readBuf.length);

		public QueueInputStream(SpscOffHeapQueue queue) {
			this.queue = queue;
			wrapped.order(ByteOrder.LITTLE_ENDIAN);
		}

		@Override
		public int read() throws IOException {
			int r = read(readBuf, 0, 1);
			if (r == 1) {
				return readBuf[0];
			}
			return -1;
		}

		@Override
		public int read(byte[] b, int off, int size) throws IOException {
			int readed = 0;
			while (readed < size) {
				int len;
				try {
					len = readOnce(b, readed + off, size - readed);
				} catch (InterruptedException e) {
					InterruptedIOException interruptedIOException = new InterruptedIOException(e.getMessage());
					interruptedIOException.bytesTransferred = readed;
					throw interruptedIOException;
				}
				if (len < 0) {
					return readed > 0 ? readed : -1;
				}
				readed += len;
			}

			return readed;
		}

		public int read(byte[] b, int size) throws IOException {
			return read(b, 0, size);
		}

		private int readOnce(byte[] b, int off, int len) throws InterruptedException {
			//as read address will only be written by this consumer,
			// thus, no need to enforce atomic for this variable
			final long currentRead = queue.getReadPlain();
			long writeCache = queue.getWriteCache();
			//if read catches write cache, means there are no data to read
			while (currentRead >= writeCache) {
				//get write must be careful about memory order as it is written by another thread
				queue.setWriteCache(queue.getWrite());
				writeCache = queue.getWriteCache();
				if (currentRead >= writeCache) {
					if (queue.isFinished()) {
						//double check write ptr as there maybe an writer update between getWrite() and isFinished.
						//we only ensure the memory ordering, so, if we see finish flag, then we must can see the final write flag
						queue.setWriteCache(queue.getWrite());
						writeCache = queue.getWriteCache();
						if (currentRead >= writeCache) {
							return -1;
						}
						break;
					}
					Thread.sleep(0);
				}
			}

			//check whether there is a wrap in ring buffer read
			long nextReadWrap = nextWrap(currentRead, queue.getCapacity());

			int avail = writeCache > nextReadWrap ? (int) (nextReadWrap - currentRead)
				: (int) (writeCache - currentRead);
			int bytesToRead = Math.min(avail, len);
			UNSAFE.copyMemory(null, queue.getArrayBase() + (currentRead & queue.getMask()),
				b, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, bytesToRead);
			queue.setRead(currentRead + bytesToRead);
			return bytesToRead;
		}

		@Override
		public int available() {
			//as read address will only be written by this consumer,
			// thus, no need to enforce atomic for this variable
			final long currentRead = queue.getReadPlain();
			long writeCache = queue.getWriteCache();
			//if read catches write cache, means there are no data to read
			if (currentRead >= writeCache) {
				//get write must be careful about memory order as it is written by another thread
				queue.setWriteCache(queue.getWrite());
				writeCache = queue.getWriteCache();
			}

			int avail = (int) (writeCache - currentRead);
			if (avail > 0) {
				return avail;
			}
			return 0;
		}

		public int getInt() throws IOException {
			read(readBuf, 4);
			wrapped.clear();
			return wrapped.getInt();
		}

		public short getShort() throws IOException {
			read(readBuf, 2);
			wrapped.clear();
			return wrapped.getShort();
		}

		public long getLong() throws IOException {
			read(readBuf, 8);
			wrapped.clear();
			return wrapped.getLong();
		}

		public double getDouble() throws IOException {
			read(readBuf, 8);
			wrapped.clear();
			return wrapped.getDouble();
		}

		public float getFloat() throws IOException {
			read(readBuf, 4);
			wrapped.clear();
			return wrapped.getFloat();
		}
	}

	public static class QueueOutputStream extends OutputStream {
		private static Logger LOG = LoggerFactory.getLogger(QueueOutputStream.class);

		private SpscOffHeapQueue queue;
		private byte[] writeBuf = new byte[10 * 1024];
		ByteBuffer bb = ByteBuffer.wrap(writeBuf, 0, writeBuf.length);

		public QueueOutputStream(SpscOffHeapQueue queue) {
			this.queue = queue;
			bb.order(ByteOrder.LITTLE_ENDIAN);
			//LOG.info("Queue info: "+queue.toString());
		}

		@Override
		public void write(int b) throws IOException {
			writeBuf[0] = (byte) (b & 0xff);
			write(writeBuf, 0, 1);
		}

		@Override
		public void write(byte[] b, int off, int len) {
			long currentWrite = queue.getWritePlain();
			final long readWatermark = currentWrite - (queue.getCapacity() - len);
			//if the ring queue is full as read leaves behind too much
			//wait until there is enough space to write
			while (queue.getReadCache() <= readWatermark) {
				queue.setReadCache(queue.getRead());
				if (queue.getReadCache() <= readWatermark) {
					Thread.yield();
				}
			}

			int written = 0;
			while (written < len) {
				long nextWriteWrap = nextWrap(currentWrite, queue.getCapacity());
				int bytesToWrite = Math.min(len - written, (int) (nextWriteWrap - currentWrite));
				UNSAFE.copyMemory(b, Unsafe.ARRAY_BYTE_BASE_OFFSET + off + written,
					null, queue.getArrayBase() + (currentWrite & queue.getMask()),
					bytesToWrite);

				queue.setWrite(currentWrite + bytesToWrite);
				written += bytesToWrite;
				currentWrite += bytesToWrite;
			}
			queue.setWrite(currentWrite);
		}

		public boolean tryReserve(int len) {
			long currentWrite = queue.getWritePlain();
			final long readWatermark = currentWrite - (queue.getCapacity() - len);
			if (queue.getReadCache() <= readWatermark) {
				queue.setReadCache(queue.getRead());
			}
			//if the ring queue is full as read leaves behind too much
			//means there is no enough space
			long readCache = queue.getReadCache();
			return readCache > readWatermark;
		}

		@Override
		public void close() {
			queue.markFinished();
		}

		public void putByte(byte v) throws IOException {
			write(v);
		}

		public void putShort(short v) throws IOException {
			bb.clear();
			bb.putShort(v);
			write(writeBuf, 0, 2);
		}

		public void putInt(int v) throws IOException {
			bb.clear();
			bb.putInt(v);
			write(writeBuf, 0, 4);
		}

		public void putLong(long v) throws IOException {
			bb.clear();
			bb.putLong(v);
			write(writeBuf, 0, 8);
		}

		public void putDouble(double v) throws IOException {
			bb.clear();
			bb.putDouble(v);
			write(writeBuf, 0, 8);
		}

		public void putFloat(float v) throws IOException {
			bb.clear();
			bb.putFloat(v);
			write(writeBuf, 0, 4);
		}

		public void putByteArray(byte[] arr) throws IOException {
			putInt(arr.length);
			write(arr);
		}
	}
}
