package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.TokenizerMapper;
import com.alibaba.alink.params.nlp.TokenizerParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Transform all words into lower case, and remove extra space.
 */
public class Tokenizer extends MapTransformer<Tokenizer>
	implements TokenizerParams <Tokenizer> {

	public Tokenizer() {
		this(null);
	}

	public Tokenizer(Params params) {
		super(TokenizerMapper::new, params);
	}
}
