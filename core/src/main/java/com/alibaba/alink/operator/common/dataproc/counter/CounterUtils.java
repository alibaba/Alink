package com.alibaba.alink.operator.common.dataproc.counter;

import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;

import java.io.Serializable;

import static org.apache.flink.shaded.guava18.com.google.common.hash.Hashing.murmur3_128;

/**
 * @author lqb
 * @date 2018/10/19
 */
public class CounterUtils implements Serializable {
	private static final long serialVersionUID = 6098036165865645301L;
	public static double EMPTYBUCKETRATE = 0.051;
	public static double POW_2_32 = 4294967296.0;
	public static double NEGATIVE_POW_2_32 = -4294967296.0;
	public static int KNN = 6;
	public static int BYTE_LENGTH = 8;
	public static HashFunction hashFunc = murmur3_128(0);

	public static long linearCounting(int zeros, int length) {
		return Math.round(length * Math.log((double) length / zeros));
	}

	public static Long hash128(Object o) {
		if (o == null) {
			return 0L;
		} else if (o instanceof String) {
			final byte[] bytes = ((String) o).getBytes();
			return hashFunc.hashBytes(bytes).asLong();
		} else if (o instanceof byte[]) {
			final byte[] bytes = (byte[]) o;
			return hashFunc.hashBytes(bytes).asLong();
		}
		return hashFunc.hashUnencodedChars(o.toString()).asLong();
	}

}
