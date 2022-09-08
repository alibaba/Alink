package com.alibaba.alink.operator.local.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.nlp.TokenizerMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.nlp.TokenizerParams;

/**
 * Transform all words into lower case, and remove extra space.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("文本分解")
public final class TokenizerLocalOp extends MapLocalOp <TokenizerLocalOp>
	implements TokenizerParams <TokenizerLocalOp> {

	public TokenizerLocalOp() {
		this(null);
	}

	public TokenizerLocalOp(Params params) {
		super(TokenizerMapper::new, params);
	}
}
