package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ModelStreamScanParams;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.shared.optim.HasLearningRateDefaultAsNull;
import com.alibaba.alink.params.validators.RangeValidator;

public interface OnlineLearningTrainParams<T> extends WithParams <T>,
	HasTimeIntervalDefaultAs1800 <T>,
	ModelStreamScanParams <T>,
	HasLearningRateDefaultAsNull <T>,
	HasL1DefaultAs01 <T>,
	HasL2DefaultAs01 <T>,
	HasAlpha <T>,
	HasBeta <T> {

	@NameCn("gamma")
	@DescCn("gamma: parameter for RMSProp or momentum optimizer.")
	ParamInfo <Double> GAMMA = ParamInfoFactory
		.createParamInfo("gamma", Double.class)
		.setDescription("gamma: parameter for RMSProp or momentum optimizer.")
		.setHasDefaultValue(0.9)
		.setValidator(new RangeValidator <>(0.0, 1.0))
		.build();

	default Double getGamma() {
		return get(GAMMA);
	}

	default T setGamma(Double value) {
		return set(GAMMA, value);
	}

	@NameCn("beta1")
	@DescCn("beta1: parameter for adam optimizer.")
	ParamInfo <Double> BETA_1 = ParamInfoFactory
		.createParamInfo("beta1", Double.class)
		.setDescription("beta: parameter for adam optimizer.")
		.setHasDefaultValue(0.9)
		.setValidator(new RangeValidator <>(0.0, 1.0))
		.build();

	default Double getBeta1() {
		return get(BETA_1);
	}

	default T setBeta1(Double value) {
		return set(BETA_1, value);
	}

	@NameCn("beta2")
	@DescCn("beta2: parameter for adam optimizer.")
	ParamInfo <Double> BETA_2 = ParamInfoFactory
		.createParamInfo("beta2", Double.class)
		.setDescription("beta: parameter for adam optimizer.")
		.setHasDefaultValue(0.999)
		.setValidator(new RangeValidator <>(0.0, 1.0))
		.build();

	default Double getBeta2() {
		return get(BETA_2);
	}

	default T setBeta2(Double value) {
		return set(BETA_2, value);
	}

	@NameCn("优化方法")
	@DescCn("在线学习问题求解时选择的优化方法")
	ParamInfo <OptimMethod> OPTIM_METHOD = ParamInfoFactory
		.createParamInfo("optimMethod", OptimMethod.class)
		.setDescription("optimization method, which can be FTRL, ADAM, RMSProp, ADAGRAD, SGD, MOMENTUM.")
		.setHasDefaultValue(OptimMethod.FTRL)
		.build();

	default OptimMethod getOptimMethod() {
		return get(OPTIM_METHOD);
	}

	default T setOptimMethod(String value) {
		return set(OPTIM_METHOD, ParamUtil.searchEnum(OPTIM_METHOD, value));
	}

	default T setOptimMethod(OptimMethod value) {
		return set(OPTIM_METHOD, value);
	}

	/**
	 * Optimization Type.
	 */
	enum OptimMethod {
		FTRL,
		ADAM,
		RMSprop,
		ADAGRAD,
		SGD,
		MOMENTUM
	}
}
