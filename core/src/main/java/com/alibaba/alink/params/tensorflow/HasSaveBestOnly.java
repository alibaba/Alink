package com.alibaba.alink.params.tensorflow;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasSaveBestOnly<T> extends WithParams <T> {
	@NameCn("是否导出最优的 checkpoint")
	@DescCn("是否导出最优的 checkpoint，仅在总并发度为 1 时生效")
	ParamInfo <Boolean> SAVE_BEST_ONLY = ParamInfoFactory
		.createParamInfo("saveBestOnly", Boolean.class)
		.setDescription("Whether to export the checkpoint with best metric, only works when total parallelism is 1")
		.setHasDefaultValue(false)
		.build();

	default Boolean getSaveBestOnly() {
		return get(SAVE_BEST_ONLY);
	}

	default T setSaveBestOnly(Boolean value) {
		return set(SAVE_BEST_ONLY, value);
	}
}
