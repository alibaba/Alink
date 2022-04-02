package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.nlp.NGramMapper;
import com.alibaba.alink.params.nlp.NGramParams;

/**
 * Transfrom a document into a new document composed of all its ngrams. The document is splitted into
 * an array of words by a word delimiter(default space). Through sliding the word array, we get all ngrams
 * and each ngram is connected with a "_" character. All the ngrams are joined together with space in the
 * new document.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("NGram")
public class NGramBatchOp extends MapBatchOp <NGramBatchOp>
	implements NGramParams <NGramBatchOp> {

	private static final long serialVersionUID = -5377006723757981207L;

	public NGramBatchOp() {
		this(null);
	}

	public NGramBatchOp(Params params) {
		super(NGramMapper::new, params);
	}

}
