package com.alibaba.alink.operator.common.io.partition;

import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.io.filesystem.FilePath;

import java.io.IOException;
import java.io.Serializable;

public interface SinkCollectorCreator extends Serializable {
	Collector <Row> createCollector(FilePath filePath) throws IOException;
}
