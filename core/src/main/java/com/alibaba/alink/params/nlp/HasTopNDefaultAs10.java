package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.MinValidator;

public interface HasTopNDefaultAs10<T> extends
	WithParams <T> {
	@NameCn("前N的数据")
	@DescCn("挑选最近的N个数据")
	ParamInfo <Integer> TOP_N = ParamInfoFactory
		.createParamInfo("topN", Integer.class)
		.setDescription("top n")
		.setHasDefaultValue(10)
		.setValidator(new MinValidator <>(1))
		.build();

	default Integer getTopN() {
		return get(TOP_N);
	}

	default T setTopN(Integer value) {
		return set(TOP_N, value);
	}
}
