package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.shared.iter.HasMaxIter;

public interface EsdDetectorParams<T> extends
	WithUniVarParams <T>,
	HasMaxIter <T> {

	@NameCn("置信度")
	@DescCn("置信度")
	ParamInfo <Double> ALPHA = ParamInfoFactory
		.createParamInfo("alpha", Double.class)
		.setDescription("confidence level")
		.setHasDefaultValue(0.05)
		.build();

	default Double getAlpha() {return get(ALPHA);}

	default T setAlpha(Double value) {return set(ALPHA, value);}
}
