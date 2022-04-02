package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.nlp.TokenizerMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.nlp.TokenizerParams;

/**
 * Transform all words into lower case, and remove extra space.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("文本分解")
public final class TokenizerStreamOp extends MapStreamOp <TokenizerStreamOp>
	implements TokenizerParams <TokenizerStreamOp> {

	private static final long serialVersionUID = -5428492033431991388L;

	public TokenizerStreamOp() {
		this(null);
	}

	public TokenizerStreamOp(Params params) {
		super(TokenizerMapper::new, params);
	}
}
