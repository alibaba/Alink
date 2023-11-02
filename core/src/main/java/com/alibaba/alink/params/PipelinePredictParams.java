package com.alibaba.alink.params;

import com.alibaba.alink.params.mapper.MapperParams;
import com.alibaba.alink.params.shared.HasModelFilePath;

public interface PipelinePredictParams<T> extends MapperParams <T>, HasModelFilePath<T> {}
