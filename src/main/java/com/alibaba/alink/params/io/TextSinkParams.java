package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.HasOverwriteSink;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorCol;

public interface TextSinkParams<T> extends WithParams<T>,
    HasFilePath<T>,
	HasOverwriteSink<T>,
	HasNumFiles<T> {
}
