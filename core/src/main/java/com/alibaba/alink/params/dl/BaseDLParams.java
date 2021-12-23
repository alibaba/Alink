package com.alibaba.alink.params.dl;

import com.alibaba.alink.params.tensorflow.savedmodel.HasOutputSchemaStr;

public interface BaseDLParams<T> extends BasePythonParams <T>, HasNumWorkersDefaultAsNull <T>, HasOutputSchemaStr <T> {
}
