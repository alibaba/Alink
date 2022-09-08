package com.alibaba.alink.operator.local.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.nlp.NGramMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.nlp.NGramParams;

/**
 * Transfrom a document into a new document composed of all its ngrams. The document is splitted into
 * an array of words by a word delimiter(default space). Through sliding the word array, we get all ngrams
 * and each ngram is connected with a "_" character. All the ngrams are joined together with space in the
 * new document.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("NGram")
public class NGramLocalOp extends MapLocalOp <NGramLocalOp>
	implements NGramParams <NGramLocalOp> {

	public NGramLocalOp() {
		this(null);
	}

	public NGramLocalOp(Params params) {
		super(NGramMapper::new, params);
	}

}
