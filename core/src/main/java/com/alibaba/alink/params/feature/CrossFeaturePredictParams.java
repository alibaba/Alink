package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasOutputCol;

public interface CrossFeaturePredictParams<T> extends
	ModelMapperParams <T>,
	HasOutputCol <T> {
}
