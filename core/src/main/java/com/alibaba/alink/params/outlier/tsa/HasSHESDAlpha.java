package com.alibaba.alink.params.outlier.tsa;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.MinValidator;

public interface HasSHESDAlpha<T> extends WithParams <T> {

	@NameCn("Alpha")
	@DescCn("Alpha")
	ParamInfo <Double> SHESD_ALPHA = ParamInfoFactory
		.createParamInfo("shesdAlpha", Double.class)
		.setDescription("The level of statistical significance" +
			" with which to accept or reject anomalies.")
		.setHasDefaultValue(0.05)
		.setValidator(new MinValidator <>(0.0))
		.build();

	default Double getShesdAlpha() {
		return get(SHESD_ALPHA);
	}

	default T setShesdAlpha(Double value) {
		return set(SHESD_ALPHA, value);
	}
}
