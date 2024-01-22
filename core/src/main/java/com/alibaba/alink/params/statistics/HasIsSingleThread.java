package com.alibaba.alink.params.statistics;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.MinValidator;

public interface HasIsSingleThread<T> extends WithParams <T> {
	
	@NameCn("线程数")
	@DescCn("线程数")
	ParamInfo <Integer> THREAD_NUM = ParamInfoFactory
		.createParamInfo("threadNum", Integer.class)
		.setDescription("thread num.")
		.setValidator(new MinValidator <>(0))
		.build();

}

