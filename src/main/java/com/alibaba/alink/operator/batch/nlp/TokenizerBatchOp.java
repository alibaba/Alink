package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.TokenizerMapper;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.params.nlp.TokenizerParams;

/**
 * Transform all words into lower case, and remove extra space.
 */
public final class TokenizerBatchOp extends MapBatchOp <TokenizerBatchOp>
	implements TokenizerParams <TokenizerBatchOp> {

	public TokenizerBatchOp() {
		this(null);
	}

	public TokenizerBatchOp(Params params) {
		super(TokenizerMapper::new, params);
	}
}
