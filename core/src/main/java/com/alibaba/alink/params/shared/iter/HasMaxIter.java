package com.alibaba.alink.params.shared.iter;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMaxIter<T> extends WithParams <T> {

	@NameCn("最大迭代步数")
	@DescCn("最大迭代步数")
	ParamInfo <Integer> MAX_ITER = ParamInfoFactory
		.createParamInfo("maxIter", Integer.class)
		.setDescription("Maximum iterations")
		.build();

	default Integer getMaxIter() {
		return get(MAX_ITER);
	}

	default T setMaxIter(Integer value) {
		return set(MAX_ITER, value);
	}
}
