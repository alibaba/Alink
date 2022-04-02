package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasSinglePrecisionHistogram<T> extends WithParams <T> {

	@NameCn("single precision histogram")
	@DescCn("single precision histogram")
	ParamInfo <Boolean> SINGLE_PRECISION_HISTOGRAM = ParamInfoFactory
		.createParamInfo("singlePrecisionHistogram", Boolean.class)
		.setDescription("Use single precision to build histograms instead of double precision.")
		.setHasDefaultValue(false)
		.build();

	default Boolean getSinglePrecisionHistogram() {
		return get(SINGLE_PRECISION_HISTOGRAM);
	}

	default T setSinglePrecisionHistogram(Boolean singlePrecisionHistogram) {
		return set(SINGLE_PRECISION_HISTOGRAM, singlePrecisionHistogram);
	}
}
