package com.alibaba.alink.common.io.filesystem.binary;

import org.apache.flink.types.Row;

import java.io.IOException;

public interface BaseStreamRowSerializer {

	void serialize(Row row) throws IOException;

	Row deserialize() throws IOException;

}
