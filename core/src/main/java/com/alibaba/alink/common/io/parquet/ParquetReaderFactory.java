package com.alibaba.alink.common.io.parquet;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.filesystem.FilePath;

import java.io.Serializable;

public interface ParquetReaderFactory  {
	void open(FilePath filePath);
	boolean reachedEnd();
	Row nextRecord();
	void close();
	TableSchema getTableSchemaFromParquetFile(FilePath filePath);
}
