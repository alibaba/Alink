package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface TextSourceParams<T> extends WithParams<T>,
    HasFilePathWithFileSystem<T>,
	HasIgnoreFirstLine<T> ,
	HasTextCol<T> {
}
