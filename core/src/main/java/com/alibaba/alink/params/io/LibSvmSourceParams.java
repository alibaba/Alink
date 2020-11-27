package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface LibSvmSourceParams<T> extends WithParams <T>,
	HasFilePathWithFileSystem <T>, HasStartIndexDefaultAs1 <T> {
}
