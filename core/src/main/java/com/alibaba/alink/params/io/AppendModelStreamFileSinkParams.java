package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface AppendModelStreamFileSinkParams<T> extends WithParams <T>,
	ModelStreamFileSinkParams <T>,
	HasModelTimeDefaultAsNull <T>,
	HasNumFiles <T> {
}
