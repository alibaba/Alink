package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasSetStable<T> extends WithParams <T> {

	@NameCn("编码是否稳定")
	@DescCn("稳定情况下，同样的数据在每次执行时编码保持不变")
	ParamInfo <Boolean> SET_STABLE = ParamInfoFactory
		.createParamInfo("setStable", Boolean.class)
		.setDescription("to make the result of graph algo stable.")
		.setHasDefaultValue(true)
		.build();

	default Boolean getSetStable() {return get(SET_STABLE);}

	default T setSetStable(Boolean value) {return set(SET_STABLE, value);}
}
