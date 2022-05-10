package com.alibaba.alink.common.io.redis;

import java.util.List;

public interface Redis {
	void close();

	String ping();

	String set(final byte[] key, final byte[] value);
	String set(final String key, final String value);

	byte[] get(final byte[] key);

	String get(final String key);
	
	List<byte[]> getKeys();
}
