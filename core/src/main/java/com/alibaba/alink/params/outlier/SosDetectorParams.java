package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface SosDetectorParams<T> extends
	OutlierDetectorParams <T>,
	WithMultiVarParams <T> {

	@NameCn("邻近因子")
	@DescCn("邻近因子。它的近似含义是当某个点的近邻个数小于\"邻近因子\"个时，这个点的离群score会比较高。")
	ParamInfo <Double> PERPLEXITY = ParamInfoFactory
		.createParamInfo("perplexity", Double.class)
		.setDescription("Perplexity")
		.setHasDefaultValue(4.0)
		.build();

	default Double getPerplexity() {
		return get(PERPLEXITY);
	}

	default T setPerplexity(Double value) {
		return set(PERPLEXITY, value);
	}

}
