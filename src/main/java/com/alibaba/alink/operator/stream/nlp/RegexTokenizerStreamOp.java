package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.RegexTokenizerMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.nlp.RegexTokenizerParams;

/**
 * If gaps is true, it splits the document with the given pattern. If gaps is false, it extract the tokens matching the
 * pattern.
 * It processes streaming data.
 */
public final class RegexTokenizerStreamOp extends MapStreamOp <RegexTokenizerStreamOp>
	implements RegexTokenizerParams <RegexTokenizerStreamOp> {

	public RegexTokenizerStreamOp() {
		this(null);
	}

	public RegexTokenizerStreamOp(Params params) {
		super(RegexTokenizerMapper::new, params);
	}
}
