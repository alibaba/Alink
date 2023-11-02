package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasConstrainedLinearModelType<T> extends WithParams <T> {
	@NameCn("线性模型")
	@DescCn("线性模型")
	ParamInfo <LinearModelType> LINEAR_MODEL_TYPE = ParamInfoFactory
		.createParamInfo("linearModelType", LinearModelType.class)
		.setDescription("optimMethod: lr, linearReg")
		.setOptional()
		.setHasDefaultValue(LinearModelType.LR)
		.build();

	default LinearModelType getLinearModelType() {
		return get(LINEAR_MODEL_TYPE);
	}

	default T setLinearModelType(LinearModelType value) {
		return set(LINEAR_MODEL_TYPE, value);
	}

	default T setLinearModelType(String value) {
		return set(LINEAR_MODEL_TYPE, ParamUtil.searchEnum(LINEAR_MODEL_TYPE, value));
	}

	enum LinearModelType {
		LR,
		LinearReg,
		Divergence
	}

}
