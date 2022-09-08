package com.alibaba.alink.operator.local.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.nlp.RegexTokenizerMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.nlp.RegexTokenizerParams;

/**
 * If gaps is true, it splits the document with the given pattern. If gaps is false, it extract the tokens matching the
 * pattern.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("RegexTokenizer")
public final class RegexTokenizerLocalOp extends MapLocalOp <RegexTokenizerLocalOp>
	implements RegexTokenizerParams <RegexTokenizerLocalOp> {

	public RegexTokenizerLocalOp() {
		this(null);
	}

	public RegexTokenizerLocalOp(Params params) {
		super(RegexTokenizerMapper::new, params);
	}
}
