package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.HasOverwriteSink;

public interface AkSinkParams<T> extends WithParams <T>,
	HasFilePathWithFileSystem <T>,
	HasOverwriteSink <T>,
	HasNumFiles <T> {
}
