package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.MinValidator;

public interface HasBoxPlotK<T> extends WithParams <T> {
	@NameCn("检测阈值")
	@DescCn("用分位数判断异常的阈值")
	ParamInfo <Double> K = ParamInfoFactory
		.createParamInfo("k", Double.class)
		.setDescription("k")
		.setHasDefaultValue(1.5)
		.setValidator(new MinValidator <>(0.0))
		.build();

	default Double getK() {return get(K);}

	default T setK(Double value) {return set(K, value);}
}
