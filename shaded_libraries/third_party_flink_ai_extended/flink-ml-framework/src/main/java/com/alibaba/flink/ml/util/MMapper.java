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

//This class comes from https://gist.github.com/bnyeggen/c679a5ea6a68503ed19f

import sun.nio.ch.FileChannelImpl;

import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;

/**
 * share memory map
 */
@SuppressWarnings("restriction")
public class MMapper {

	private static final Method mmap;
	private static final Method unmmap;

	private long addr;
	private final long size;
	private final String loc;

	static {
		try {
			mmap = getMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);
			unmmap = getMethod(FileChannelImpl.class, "unmap0", long.class, long.class);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	//Bundle reflection calls to get access to the given method
	private static Method getMethod(Class<?> cls, String name, Class<?>... params) throws Exception {
		Method m = cls.getDeclaredMethod(name, params);
		m.setAccessible(true);
		return m;
	}

	//Round to next 4096 bytes
	private static long roundTo4096(long i) {
		return (i + 0xfffL) & ~0xfffL;
	}

	public MMapper(final String loc, long len) throws Exception {
		this.loc = loc;
		this.size = roundTo4096(len);
		map();
	}

	public long getSize() {
		return size;
	}

	public long getAddress() {
		return addr;
	}

	//Given that the location and size have been set, map that location
	//for the given length and set this.addr to the returned offset
	private void map() throws Exception {
		final RandomAccessFile backingFile = new RandomAccessFile(this.loc, "rw");
		backingFile.setLength(this.size);

		final FileChannel ch = backingFile.getChannel();
		this.addr = (long) mmap.invoke(ch, 1, 0L, this.size);

		ch.close();
		backingFile.close();
	}

	public void unmmap() throws Exception {
		if (addr != 0) {
			unmmap.invoke(null, addr, this.size);
		}
		addr = 0;
	}

	public void close() throws Exception {
		unmmap();
	}
}
