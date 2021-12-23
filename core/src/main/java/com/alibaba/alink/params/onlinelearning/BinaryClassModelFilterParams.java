package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.linear.HasPositiveLabelValueString;

public interface BinaryClassModelFilterParams<T> extends WithParams <T>,
	HasLabelCol<T>,
	HasPositiveLabelValueString <T> {

	/**
	 * @cn-name 模型过滤的Auc阈值
	 * @cn 模型过滤的Auc阈值
	 */
	ParamInfo <Double> AUC_THRESHOLD = ParamInfoFactory
		.createParamInfo("aucThreshold", Double.class)
		.setDescription("auc threshold")
		.setHasDefaultValue(0.5)
		.build();

	default Double getAucThreshold() {return get(AUC_THRESHOLD);}

	default T setAucThreshold(Double value) {return set(AUC_THRESHOLD, value);}

	/**
	 * @cn-name 模型过滤的Accuracy阈值
	 * @cn 模型过滤的Accuracy阈值
	 */
	ParamInfo <Double> ACCURACY_THRESHOLD = ParamInfoFactory
		.createParamInfo("accuracyThreshold", Double.class)
		.setDescription("accuracy threshold")
		.setHasDefaultValue(0.5)
		.build();

	default Double getAccuracyThreshold() {return get(ACCURACY_THRESHOLD);}

	default T setAccuracyThreshold(Double value) {return set(ACCURACY_THRESHOLD, value);}
}
