package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasEta<T> extends WithParams <T> {

	@NameCn("学习率")
	@DescCn("学习率")
	ParamInfo <Double> ETA = ParamInfoFactory
		.createParamInfo("eta", Double.class)
		.setDescription("Step size shrinkage used in update to prevents overfitting. "
			+ "After each boosting step, we can directly get the weights of new features, "
			+ "and eta shrinks the feature weights to make the boosting process more conservative.")
		.setHasDefaultValue(0.3)
		.build();

	default Double getEta() {
		return get(ETA);
	}

	default T setEta(Double eta) {
		return set(ETA, eta);
	}
}
