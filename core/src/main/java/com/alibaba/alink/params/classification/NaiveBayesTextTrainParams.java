package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.shared.HasSmoothing;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * Parameters of text naive bayes training process.
 */
public interface NaiveBayesTextTrainParams<T> extends
        HasLabelCol<T>,
        HasWeightColDefaultAsNull<T>,
        HasVectorCol<T>,
        HasSmoothing<T> {

	ParamInfo<String> MODEL_TYPE = ParamInfoFactory
		.createParamInfo("modelType", String.class)
		.setDescription("model type : Multinomial or Bernoulli.")
		.setHasDefaultValue("Multinomial")
		.setAlias(new String[] {"bayesType"})
		.build();

	default String getModelType() {
		return get(MODEL_TYPE);
	}

	default T setModelType(String value) {
		return set(MODEL_TYPE, value);
	}

}
