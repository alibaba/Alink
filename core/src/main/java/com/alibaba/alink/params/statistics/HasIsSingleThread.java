package com.alibaba.alink.params.statistics;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasIsSingleThread<T> extends WithParams <T> {
	
	@NameCn("是否单线程")
	@DescCn("是否单线程")
	ParamInfo <Boolean> IS_SINGLE_THREAD = ParamInfoFactory
		.createParamInfo("isSingleThread", Boolean.class)
		.setDescription("is single thread.")
		.setHasDefaultValue(false)
		.build();

}

