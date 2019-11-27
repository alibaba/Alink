package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Params minDF.When the number of documents a word appears in is below minDF, the word will not be included in the
 * dictionary.
 *
 * It could be an exact count or a fraction of the document number count. When minDF is within [0, 1), it's used as a fraction.
 */
public interface HasMinDF<T> extends WithParams<T> {
	ParamInfo <Double> MIN_DF = ParamInfoFactory
		.createParamInfo("minDF", Double.class)
		.setDescription(
			"When the number of documents a word appears in is below minDF, the word will not be included in the "
				+ "dictionary. It could be an exact count"
				+ "or a fraction of the document number count. When minDF is within [0, 1), it's used as a fraction.")
		.setHasDefaultValue(1.0)
		.build();

	default double getMinDF() {
		return get(MIN_DF);
	}

	default T setMinDF(Double value) {
		return set(MIN_DF, value);
	}
}
