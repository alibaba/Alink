package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.nlp.TokenizerMapper;
import com.alibaba.alink.params.nlp.TokenizerParams;

/**
 * Transform all words into lower case, and remove extra space.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("文本分解")
public final class TokenizerBatchOp extends MapBatchOp <TokenizerBatchOp>
	implements TokenizerParams <TokenizerBatchOp> {

	private static final long serialVersionUID = 121579072591115285L;

	public TokenizerBatchOp() {
		this(null);
	}

	public TokenizerBatchOp(Params params) {
		super(TokenizerMapper::new, params);
	}
}
