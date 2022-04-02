package com.alibaba.alink.python.utils;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.InputStream;

@SuppressWarnings("unused")
public class DataStreamReadUtil {

	@SuppressWarnings("unused")
	public static Tuple2 <Integer, byte[]> read(InputStream stream, int count, int offset) throws IOException {
		byte[] bytes = new byte[count];
		int numBytesRead = stream.read(bytes, offset, count);
		return Tuple2.of(numBytesRead, bytes);
	}
}
