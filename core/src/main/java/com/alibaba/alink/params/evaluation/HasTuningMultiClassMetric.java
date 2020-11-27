package com.alibaba.alink.params.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.operator.common.evaluation.TuningMultiClassMetric;
import com.alibaba.alink.params.ParamUtil;

public interface HasTuningMultiClassMetric<T> extends WithParams <T> {
	ParamInfo <TuningMultiClassMetric> TUNING_MULTI_CLASS_METRIC = ParamInfoFactory
		.createParamInfo("tuningMultiClassMetric", TuningMultiClassMetric.class)
		.setDescription("metric of multi-class evaluation in tuning")
		.setRequired()
		.build();

	default TuningMultiClassMetric getTuningMultiClassMetric() {
		return get(TUNING_MULTI_CLASS_METRIC);
	}

	default T setTuningMultiClassMetric(TuningMultiClassMetric metric) {
		return set(TUNING_MULTI_CLASS_METRIC, metric);
	}

	default T setTuningMultiClassMetric(String metric) {
		return set(TUNING_MULTI_CLASS_METRIC, ParamUtil.searchEnum(TUNING_MULTI_CLASS_METRIC, metric));
	}
}
