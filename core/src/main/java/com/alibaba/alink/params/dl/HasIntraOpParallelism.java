package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasIntraOpParallelism<T> extends WithParams <T> {
	/**
	 * @cn Op 间并发度
	 * @cn-name Op 间并发度
	 */
	ParamInfo<Integer> INTRA_OP_PARALLELISM = ParamInfoFactory
		.createParamInfo("intraOpParallelism", Integer.class)
		.setDescription("Intra-Op parallelism")
		.setHasDefaultValue(4)
		.build();

	default Integer getIntraOpParallelism() {
		return get(INTRA_OP_PARALLELISM);
	}

	default T setIntraOpParallelism(Integer value) {
		return set(INTRA_OP_PARALLELISM, value);
	}
}
