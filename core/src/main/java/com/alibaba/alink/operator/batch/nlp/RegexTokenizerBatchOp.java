package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.nlp.RegexTokenizerMapper;
import com.alibaba.alink.params.nlp.RegexTokenizerParams;

/**
 * If gaps is true, it splits the document with the given pattern. If gaps is false, it extract the tokens matching the
 * pattern.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("RegexTokenizer")
public final class RegexTokenizerBatchOp extends MapBatchOp <RegexTokenizerBatchOp>
	implements RegexTokenizerParams <RegexTokenizerBatchOp> {

	private static final long serialVersionUID = -7249487587825597293L;

	public RegexTokenizerBatchOp() {
		this(null);
	}

	public RegexTokenizerBatchOp(Params params) {
		super(RegexTokenizerMapper::new, params);
	}
}
