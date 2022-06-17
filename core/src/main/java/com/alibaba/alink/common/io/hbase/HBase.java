package com.alibaba.alink.common.io.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public interface HBase {
	void close() throws IOException;

	void createTable(String tableName, String... columnFamilies) throws IOException;

	void set(String tableName, String rowKey, String familyName, String column, byte[] data) throws IOException;

	void set(String tableName, String rowKey, String familyName, Map<String, byte[]> dataMap) throws IOException;

	void set(String tableName, String rowKey, List<String> familyNames, List<Map<String, byte[]>> dataMap) throws IOException;

	byte[] getColumn(String tableName, String rowKey, String familyName, String column) throws IOException;

	Map<String, byte[]> getFamilyColumns(String tableName, String rowKey, String familyName) throws IOException;

	Map<byte[], NavigableMap <byte[], byte[]>> getRow(String tableName, String rowKey) throws IOException;
}
