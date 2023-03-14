package com.alibaba.alink.params.outlier.tsa;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.MinValidator;

public interface HasThreshold<T> extends WithParams <T> {
	@NameCn("检测阈值")
	@DescCn("用来指导异常值的判断")
	ParamInfo <Double> THRESHOLD = ParamInfoFactory
		.createParamInfo("threshold", Double.class)
		.setDescription("threshold")
		.setHasDefaultValue(3.5)
		.setValidator(new MinValidator <>(0.0))
		.build();

	default Double getThreshold() {return get(THRESHOLD);}

	default T setThreshold(Double value) {return set(THRESHOLD, value);}
}
