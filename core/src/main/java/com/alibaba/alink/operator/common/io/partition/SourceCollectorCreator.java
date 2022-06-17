package com.alibaba.alink.operator.common.io.partition;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.io.filesystem.FilePath;

import java.io.IOException;
import java.io.Serializable;

public interface SourceCollectorCreator extends Serializable {
	TableSchema schema();

	void collect(FilePath filePath, Collector <Row> collector) throws IOException;
}
