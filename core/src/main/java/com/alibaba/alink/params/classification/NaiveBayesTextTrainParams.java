package com.alibaba.alink.params.classification;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.shared.HasSmoothing;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;

/**
 * Parameters of text naive bayes training process.
 */
public interface NaiveBayesTextTrainParams<T> extends
	HasLabelCol <T>,
	HasWeightColDefaultAsNull <T>,
	HasVectorCol <T>,
	HasSmoothing <T> {

	@NameCn("模型类型")
	@DescCn("取值为 Multinomial 或 Bernoulli")
	ParamInfo <ModelType> MODEL_TYPE = ParamInfoFactory
		.createParamInfo("modelType", ModelType.class)
		.setDescription("model type : Multinomial or Bernoulli.")
		.setHasDefaultValue(ModelType.Multinomial)
		.setAlias(new String[] {"bayesType"})
		.build();

	default ModelType getModelType() {
		return get(MODEL_TYPE);
	}

	default T setModelType(ModelType value) {
		return set(MODEL_TYPE, value);
	}

	default T setModelType(String value) {
		return set(MODEL_TYPE, ParamUtil.searchEnum(MODEL_TYPE, value));
	}

	enum ModelType {
		/**
		 * Multinomial type.
		 */
		Multinomial,

		/**
		 * Bernoulli type.
		 */
		Bernoulli
	}
}
