package com.alibaba.alink.params.tuning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface ParallelTuningMode<T> extends WithParams <T> {
	@NameCn("并行模式")
	@DescCn("是否使用并行模式，并行模式使用单节点训练加速搜索过程，数据量大时谨慎使用")
	ParamInfo <Boolean> PARALLEL_TUNING_MODE = ParamInfoFactory
		.createParamInfo("parallelTuningMode", Boolean.class)
		.setDescription("use parallel tuning mode or not, training local, only for small dataset")
		.setHasDefaultValue(false)
		.build();

	default Boolean getParallelTuningMode() {
		return get(PARALLEL_TUNING_MODE);
	}

	default T setParallelTuningMode(Boolean value) {
		return set(PARALLEL_TUNING_MODE, value);
	}
}
