package com.alibaba.alink.params.tuning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasNumFolds<T> extends WithParams <T> {
	@NameCn("交叉验证的参数")
	@DescCn("交叉验证的参数，数据的折数（大于等于2）。")
	ParamInfo <Integer> NUM_FOLDS = ParamInfoFactory
		.createParamInfo("NumFolds", Integer.class)
		.setDescription("Number of folds for cross validation (>= 2)")
		.setHasDefaultValue(10)
		.build();

	default Integer getNumFolds() {
		return get(NUM_FOLDS);
	}

	default T setNumFolds(Integer value) {
		return set(NUM_FOLDS, value);
	}
}
