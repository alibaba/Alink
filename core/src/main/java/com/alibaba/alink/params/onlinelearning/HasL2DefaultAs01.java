package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.MinValidator;

/**
 * the L2-regularized parameter.
 */
public interface HasL2DefaultAs01<T> extends WithParams <T> {

	@NameCn("正则化系数")
	@DescCn("L2 正则化系数，默认为0.1。")
	ParamInfo <Double> L_2 = ParamInfoFactory
		.createParamInfo("l2", Double.class)
		.setDescription("the L2-regularized parameter.")
		.setHasDefaultValue(0.1)
		.setValidator(new MinValidator <>(0.0))
		.setAlias(new String[] {"L2"})
		.build();

	default Double getL2() {
		return get(L_2);
	}

	default T setL2(Double value) {
		return set(L_2, value);
	}
}
