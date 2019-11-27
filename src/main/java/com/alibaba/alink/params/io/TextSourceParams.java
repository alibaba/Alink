package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface TextSourceParams<T> extends WithParams<T>,
    HasFilePath<T>,
	HasIgnoreFirstLine<T> ,
	HasTextCol<T> {
}
