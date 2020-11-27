package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface TsvSourceParams<T> extends WithParams <T>,
	HasFilePathWithFileSystem <T>,
	HasSchemaStr <T>,
	HasSkipBlinkLineDefaultAsTrue <T>,
	HasIgnoreFirstLine <T> {
}
