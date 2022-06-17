package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.io.shared.HasPartitions;

public interface LibSvmSourceParams<T> extends WithParams <T>,
	HasFilePath <T>, HasStartIndexDefaultAs1 <T>, HasPartitions<T> {
}
