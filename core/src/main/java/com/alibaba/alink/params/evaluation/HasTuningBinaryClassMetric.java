package com.alibaba.alink.params.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.evaluation.TuningBinaryClassMetric;
import com.alibaba.alink.params.ParamUtil;

public interface HasTuningBinaryClassMetric<T> extends WithParams <T> {

	@NameCn("调优二分类度量")
	@DescCn("二分类评估调优的度量参数")
	ParamInfo <TuningBinaryClassMetric> TUNING_BINARY_CLASS_METRIC = ParamInfoFactory
		.createParamInfo("tuningBinaryClassMetric", TuningBinaryClassMetric.class)
		.setDescription("metric of binary-class evaluation in tuning")
		.setRequired()
		.build();

	default TuningBinaryClassMetric getTuningBinaryClassMetric() {
		return get(TUNING_BINARY_CLASS_METRIC);
	}

	default T setTuningBinaryClassMetric(TuningBinaryClassMetric metric) {
		return set(TUNING_BINARY_CLASS_METRIC, metric);
	}

	default T setTuningBinaryClassMetric(String metric) {
		return set(TUNING_BINARY_CLASS_METRIC, ParamUtil.searchEnum(TUNING_BINARY_CLASS_METRIC, metric));
	}
}
