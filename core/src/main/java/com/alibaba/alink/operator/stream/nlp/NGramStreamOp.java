package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.nlp.NGramMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.nlp.NGramParams;

/**
 * Transfrom a document into a new document composed of all its ngrams. The document is splitted into
 * an array of words by a word delimiter(default space). Through sliding the word array, we get all ngrams
 * and each ngram is connected by a "_" character. All the ngrams are joined together with space in the
 * new document.
 * It processes streaming data.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("NGram")
public final class NGramStreamOp extends MapStreamOp <NGramStreamOp>
	implements NGramParams <NGramStreamOp> {

	private static final long serialVersionUID = -1986064720969918631L;

	public NGramStreamOp() {
		this(null);
	}

	public NGramStreamOp(Params params) {
		super(NGramMapper::new, params);
	}
}
