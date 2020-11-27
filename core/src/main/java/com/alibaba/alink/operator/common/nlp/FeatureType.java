package com.alibaba.alink.operator.common.nlp;

import com.alibaba.alink.common.utils.Functional;

import java.io.Serializable;

/**
 * FeatureType for DocCountVectorizer.
 */
public enum FeatureType implements Serializable {
	/**
	 * IDF type, the output value is inverse document frequency.
	 */
	IDF(
		(idf, termFrequency, tokenRatio) -> idf
	),
	/**
	 * WORD_COUNT type, the output value is the word count.
	 */
	WORD_COUNT(
		(idf, termFrequency, tokenRatio) -> termFrequency
	),
	/**
	 * TF_IDF type, the output value is term frequency * inverse document frequency.
	 */
	TF_IDF(
		(idf, termFrequency, tokenRatio) -> idf * termFrequency * tokenRatio
	),
	/**
	 * BINARY type, the output value is 1.0.
	 */
	BINARY(
		(idf, termFrequency, tokenRatio) -> 1.0
	),
	/**
	 * TF type, the output value is term frequency.
	 */
	TF(
		(idf, termFrequency, tokenRatio) -> termFrequency * tokenRatio
	);

	public final Functional.SerializableTriFunction <Double, Double, Double, Double> featureValueFunc;

	FeatureType(Functional.SerializableTriFunction <Double, Double, Double, Double> featureValueFunc) {
		this.featureValueFunc = featureValueFunc;
	}
}
