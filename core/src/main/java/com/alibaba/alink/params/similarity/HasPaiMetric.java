package com.alibaba.alink.params.similarity;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.operator.common.similarity.Metric;

public interface HasPaiMetric<T> extends WithParams <T> {

	/**
	 * @cn-name 度量类型
	 * @cn 计算距离时，可以取不同的度量
	 */
	ParamInfo <String> PAI_METRIC = ParamInfoFactory
		.createParamInfo("paiMetric", String.class)
		.setDescription("Method to calculate calc or distance.")
		.setHasDefaultValue(Metric.LEVENSHTEIN_SIM.name())
		.setAlias(new String[] {"method", "similarityMethod", "distanceType"})
		.build();

	default String getPaiMetric() {
		return get(PAI_METRIC);
	}

	default T setPaiMetric(String value) {
		return set(PAI_METRIC, value);
	}

}
