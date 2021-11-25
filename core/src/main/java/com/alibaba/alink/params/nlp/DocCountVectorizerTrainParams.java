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
	HasFeatureType <T>,
	HasVocabSize <T>,
	HasMinTF <T> {
	/**
	 * @cn-name 最大词频
	 * @cn 如果一个词出现的文档次数大于maxDF, 这个词不会被包含在字典中。maxDF可以是具体的词频也可以是整体词频的比例，如果minDF在[0,1)区间，会被认为是比例。
	 */
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
