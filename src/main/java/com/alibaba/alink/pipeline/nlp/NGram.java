package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.NGramMapper;
import com.alibaba.alink.params.nlp.NGramParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Transform a document into a new document composed of all its ngrams. The document is splitted into
 * an array of words by a word delimiter(default space). Through sliding the word array, we get all ngrams
 * and each ngram is connected by a "_" character. All the ngrams are joined together with space in the
 * new document.
 */
public class NGram extends MapTransformer<NGram>
	implements NGramParams <NGram> {

	public NGram() {
		this(null);
	}

	public NGram(Params params) {
		super(NGramMapper::new, params);
	}
}
