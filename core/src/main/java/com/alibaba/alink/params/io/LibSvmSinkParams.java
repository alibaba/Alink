package com.alibaba.alink.params.io;

import com.alibaba.alink.params.shared.HasOverwriteSink;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface LibSvmSinkParams<T> extends WithParams<T>,
    HasFilePath<T>, HasOverwriteSink<T>, HasVectorCol<T>, HasLabelCol<T> {
}
