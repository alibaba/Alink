package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasConstrainedOptimizationMethod<T> extends WithParams <T> {
	@NameCn("优化方法")
	@DescCn("求解优化问题时选择的优化方法")
	ParamInfo <ConstOptimMethod> CONST_OPTIM_METHOD = ParamInfoFactory
		.createParamInfo("constOptimMethod", ConstOptimMethod.class)
		.setAlias(new String[] {"optimMethod"})
		.setDescription("constrained optimization method")
		.setHasDefaultValue(ConstOptimMethod.SQP)
		.build();

	default ConstOptimMethod getConstOptimMethod() {
		return get(CONST_OPTIM_METHOD);
	}

	default T setConstOptimMethod(ConstOptimMethod value) {
		return set(CONST_OPTIM_METHOD, value);
	}

	default T setConstOptimMethod(String value) {
		return set(CONST_OPTIM_METHOD, ParamUtil.searchEnum(CONST_OPTIM_METHOD, value));
	}

	enum ConstOptimMethod {
		SQP,
		Barrier,
		LBFGS,
		Newton
	}
}
