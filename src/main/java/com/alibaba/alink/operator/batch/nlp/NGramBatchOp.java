package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.NGramMapper;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.params.nlp.NGramParams;

/**
 * Transfrom a document into a new document composed of all its ngrams. The document is splitted into
 * an array of words by a word delimiter(default space). Through sliding the word array, we get all ngrams
 * and each ngram is connected with a "_" character. All the ngrams are joined together with space in the
 * new document.
 */
public class NGramBatchOp extends MapBatchOp <NGramBatchOp>
	implements NGramParams <NGramBatchOp> {

	public NGramBatchOp() {
		this(null);
	}

	public NGramBatchOp(Params params) {
		super(NGramMapper::new, params);
	}

}
