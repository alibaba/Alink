package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasWeightCol<T> extends WithParams <T> {

	/**
	 * @cn-name 权重列名
	 * @cn 用来指定权重列, 权重列的值必须为非负的浮点数, 否则算法抛异常。
	 */
	ParamInfo <String> WEIGHT_COL = ParamInfoFactory
		.createParamInfo("weightCol", String.class)
		.setDescription("weight col name")
		.setAlias(new String[] {"weightColName", "value"})
		.setHasDefaultValue(null)
		.build();

	default String getWeightCol() {return get(WEIGHT_COL);}

	default T setWeightCol(String value) {return set(WEIGHT_COL, value);}
}
