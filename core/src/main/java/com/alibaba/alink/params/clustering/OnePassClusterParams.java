package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface OnePassClusterParams<T> extends
	HasPredictionCol <T>,
	HasPredictionDetailCol <T>,
	HasReservedColsDefaultAsNull <T> {
	@NameCn("临域距离阈值")
	@DescCn("临域距离阈值")
	ParamInfo <Double> EPSILON = ParamInfoFactory
		.createParamInfo("epsilon", Double.class)
		.setDescription("epsilon")
		.setHasDefaultValue(Double.MAX_VALUE)
		.build();

	default Double getEpsilon() {return get(EPSILON);}

	default T setEpsilon(Double value) {return set(EPSILON, value);}

	@NameCn("模型输出间隔")
	@DescCn("模型输出间隔，间隔多少条样本输出一个模型")
	ParamInfo <Integer> MODEL_OUTPUT_INTERVAL = ParamInfoFactory
		.createParamInfo("modelOutputInterval", Integer.class)
		.setDescription("Time interval of streaming windows, unit sample.")
		.setHasDefaultValue(null)
		.build();

	default Integer getModelOutputInterval() {
		return get(MODEL_OUTPUT_INTERVAL);
	}

	default T setModelOutputInterval(Integer value) {
		return set(MODEL_OUTPUT_INTERVAL, value);
	}
}
