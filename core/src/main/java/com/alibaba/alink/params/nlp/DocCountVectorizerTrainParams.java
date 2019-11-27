package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Params for DocCountVectorizerTrainBatchOp.
 */
public interface DocCountVectorizerTrainParams<T> extends
	HasSelectedCol <T>,
	HasMinDF <T>,
	HasFeatureType<T>,
	HasVocabSize<T>,
	HasMinTF<T>{
	ParamInfo <Double> MAX_DF = ParamInfoFactory
		.createParamInfo("maxDF", Double.class)
		.setDescription(
			"When the number of documents a word appears in is above maxDF, the word will not be included in the "
				+ "dictionary. It could be an exact count"
				+ "or a fraction of the document number count. When maxDF is within [0, 1), it's used as a fraction.")
		.setHasDefaultValue(Double.MAX_VALUE)
		.build();

	default double getMaxDF() {
		return get(MAX_DF);
	}

	default T setMaxDF(Double value) {
		return set(MAX_DF, value);
	}
}
