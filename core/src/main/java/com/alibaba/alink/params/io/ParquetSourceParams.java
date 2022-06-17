package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.io.shared.HasPartitions;

public interface ParquetSourceParams<T> extends WithParams <T>,
	HasFilePath <T>,
	HasPartitions<T> {
}

