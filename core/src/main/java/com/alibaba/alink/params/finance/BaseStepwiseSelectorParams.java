package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.finance.stepwiseSelector.StepWiseType;
import com.alibaba.alink.params.ParamUtil;

public interface BaseStepwiseSelectorParams<T>
	extends StepwiseSelectorParams <T>,
	HasConstrainedLinearModelType <T> {
	@NameCn("优化方法")
	@DescCn("优化问题求解时选择的优化方法")
	ParamInfo <String> OPTIM_METHOD = ParamInfoFactory
		.createParamInfo("optimMethod", String.class)
		.setDescription("optimization method")
		.setHasDefaultValue(null)
		.build();

	default String getOptimMethod() {
		return get(OPTIM_METHOD);
	}

	default T setOptimMethod(String value) {
		return set(OPTIM_METHOD, value);
	}

	ParamInfo <StepWiseType> STEP_WISE_TYPE = ParamInfoFactory
		.createParamInfo("stepWiseType", StepWiseType.class)
		.setDescription("stepwise type")
		.setHasDefaultValue(StepWiseType.fTest)
		.build();

	default StepWiseType getStepWiseType() {
		return get(STEP_WISE_TYPE);
	}

	default T setStepWiseType(StepWiseType value) {
		return set(STEP_WISE_TYPE, value);
	}

	default T setStepWiseType(String value) {
		return set(STEP_WISE_TYPE, ParamUtil.searchEnum(STEP_WISE_TYPE, value));
	}

	ParamInfo <Boolean> WITH_VIZ = ParamInfoFactory
		.createParamInfo("withViz", Boolean.class)
		.setDescription("stepwise type")
		.setOptional()
		.setHasDefaultValue(true)
		.build();

	default Boolean getWithViz() {
		return get(WITH_VIZ);
	}

	default T setWithViz(Boolean value) {
		return set(WITH_VIZ, value);
	}

}
