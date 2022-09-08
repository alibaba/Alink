package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.MinValidator;

public interface KCoreParams<T> extends
	CommonGraphParams <T> {
	@NameCn("k的数目")
	@DescCn("反复去除图中度小于等于k的点")
	ParamInfo <Integer> K = ParamInfoFactory
		.createParamInfo("k", Integer.class)
		.setDescription("k")
		.setHasDefaultValue(3)
		.setValidator(new MinValidator <>(1))
		.build();

	default Integer getK() {return get(K);}

	default T setK(Integer value) {return set(K, value);}
}
