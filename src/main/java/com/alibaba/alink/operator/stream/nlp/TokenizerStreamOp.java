package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.TokenizerMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.nlp.TokenizerParams;

/**
 * Transform all words into lower case, and remove extra space.
 */
public final class TokenizerStreamOp extends MapStreamOp <TokenizerStreamOp>
	implements TokenizerParams <TokenizerStreamOp> {

	public TokenizerStreamOp() {
		this(null);
	}

	public TokenizerStreamOp(Params params) {
		super(TokenizerMapper::new, params);
	}
}
