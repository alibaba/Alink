package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.MinValidator;

public interface HasBatchSize<T> extends WithParams <T> {

	@NameCn("batch大小")
	@DescCn("batch大小, 按行计算")
	ParamInfo <Integer> BATCH_SIZE = ParamInfoFactory
		.createParamInfo("batchSize", Integer.class)
		.setValidator(new MinValidator <>(1))
		.setDescription("batch size of sgd")
		.build();

	default Integer getBatchSize() {
		return get(BATCH_SIZE);
	}

	default T setBatchSize(Integer value) {
		return set(BATCH_SIZE, value);
	}
}
