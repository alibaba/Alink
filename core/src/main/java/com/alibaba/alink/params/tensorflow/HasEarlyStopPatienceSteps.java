package com.alibaba.alink.params.tensorflow;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasEarlyStopPatienceSteps<T> extends WithParams <T> {
	@NameCn("指标没有提升就提早停止的 step 数")
	@DescCn("经过指定的训练 step 数，如果指标没有提升，则提早停止")
	ParamInfo <Integer> EARLY_STOP_PATIENCE_STEPS = ParamInfoFactory
		.createParamInfo("earlyStopPatienceSteps", Integer.class)
		.setDescription("Number of training steps with no improvement after which training will be stopped.")
		.setHasDefaultValue(60)
		.build();

	default Integer getEarlyStopPatienceSteps() {
		return get(EARLY_STOP_PATIENCE_STEPS);
	}

	default T setEarlyStopPatienceSteps(Integer value) {
		return set(EARLY_STOP_PATIENCE_STEPS, value);
	}
}
