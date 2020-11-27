package com.alibaba.alink.params.classification;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.shared.colname.HasCategoricalCols;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import com.alibaba.alink.params.validators.MinValidator;

/**
 * Parameters of naive bayes training process.
 */
public interface NaiveBayesTrainParams<T> extends
	HasCategoricalCols <T>,
	HasFeatureCols <T>,
	HasLabelCol <T>,
	HasWeightColDefaultAsNull <T> {
	ParamInfo <Double> SMOOTHING = ParamInfoFactory
		.createParamInfo("smoothing", Double.class)
		.setDescription("the smoothing factor")
		.setHasDefaultValue(0.0)
		.setValidator(new MinValidator <>(0.0))
		.build();

	default Double getSmoothing() {
		return get(SMOOTHING);
	}

	default T setSmoothing(Double value) {
		return set(SMOOTHING, value);
	}
}
