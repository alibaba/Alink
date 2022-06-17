package com.alibaba.alink.common.io.filesystem.binary;

import org.apache.flink.types.Row;

import java.io.IOException;

public interface BaseRowSerializer {

	byte[] serialize(Row row) throws IOException;

	Row deserialize(byte[] bytes) throws IOException;
}
