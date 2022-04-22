package com.alibaba.alink.common.io.redis;

import java.util.List;

public interface Redis {
	void close();

	String ping();

	String set(final byte[] key, final byte[] value);

	byte[] get(final byte[] key);
	
	List<byte[]> getKeys();
}
