package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasContrainedOptimMethod<T> extends WithParams <T> {

	@NameCn("优化方法")
	@DescCn("优化方法")
	ParamInfo <OptimMethod> CONS_SEL_OPTIM_METHOD = ParamInfoFactory
		.createParamInfo("optimMethod", OptimMethod.class)
		.setDescription("optimMethod: sqp, barrier")
		.setOptional()
		.setHasDefaultValue(OptimMethod.SQP)
		.build();

	default OptimMethod getOptimMethod() {
		return get(CONS_SEL_OPTIM_METHOD);
	}

	default T setOptimMethod(OptimMethod value) {
		return set(CONS_SEL_OPTIM_METHOD, value);
	}

	default T setOptimMethod(String value) {
		return set(CONS_SEL_OPTIM_METHOD, ParamUtil.searchEnum(CONS_SEL_OPTIM_METHOD, value));
	}

	enum OptimMethod {
		SQP,
		BARRIER
	}

}
