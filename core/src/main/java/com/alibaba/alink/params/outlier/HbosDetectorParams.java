package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.MinValidator;

public interface HbosDetectorParams<T> extends WithMultiVarParams <T> {

	@NameCn("K")
	@DescCn("直方图 bin 的数量")
	ParamInfo <Integer> K = ParamInfoFactory
		.createParamInfo("k", Integer.class)
		.setDescription("the count of histogram bins.")
		.setHasDefaultValue(10)
		.setValidator(new MinValidator <>(1))
		.build();

	default Integer getK() {
		return get(K);
	}

	default T setK(Integer value) {
		return set(K, value);
	}
}
