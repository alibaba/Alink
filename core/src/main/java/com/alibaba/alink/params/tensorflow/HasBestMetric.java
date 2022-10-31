package com.alibaba.alink.params.tensorflow;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasBestMetric<T> extends WithParams <T> {
	@NameCn("最优指标")
	@DescCn(
		"判断模型最优时用的指标，仅在总并发度为 1 时起作用。都支持的有：loss； 二分类还支持：auc, precision, recall, binary_accuracy, false_negatives, false_positives, true_negatives, true_positives；多分类还支持：sparse_categorical_accuracy；回归还支持：mean_absolute_error, mean_absolute_percentage_error, mean_squared_error, mean_squared_logarithmic_error, root_mean_squared_error")
	ParamInfo <String> BEST_METRIC = ParamInfoFactory
		.createParamInfo("bestMetric", String.class)
		.setDescription("The metrics used to decide best checkpoint, only works when total parallelism is 1")
		.setHasDefaultValue("loss")
		.build();

	default String getBestMetric() {
		return get(BEST_METRIC);
	}

	default T setBestMetric(String value) {
		return set(BEST_METRIC, value);
	}
}
