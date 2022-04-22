package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface TFRecordDatasetSourceParams<T> extends WithParams <T>,
	HasFilePath <T>,
	HasSchemaStr <T> {
}
