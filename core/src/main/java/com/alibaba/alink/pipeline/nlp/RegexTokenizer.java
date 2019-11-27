package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.RegexTokenizerMapper;
import com.alibaba.alink.params.nlp.RegexTokenizerParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * If gaps is true, it splits the document with the given pattern. If gaps is false, it extract the tokens matching the
 * pattern.
 */
public class RegexTokenizer extends MapTransformer<RegexTokenizer>
	implements RegexTokenizerParams <RegexTokenizer> {

	public RegexTokenizer() {
		this(null);
	}

	public RegexTokenizer(Params params) {
		super(RegexTokenizerMapper::new, params);
	}
}
