package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.MinValidator;

public interface HasMaxIterDefaultAs50<T> extends WithParams <T> {
	@NameCn("最大迭代次数")
	@DescCn("最大迭代次数")
	ParamInfo <Integer> MAX_ITER = ParamInfoFactory
		.createParamInfo("maxIter", Integer.class)
		.setDescription("max iteration").setValidator(new MinValidator <>(1))
		.setHasDefaultValue(50)
		.build();

	default Integer getMaxIter() {return get(MAX_ITER);}

	default T setMaxIter(Integer value) {return set(MAX_ITER, value);}
}
