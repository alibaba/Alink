package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.StopWordsRemoverMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.nlp.StopWordsRemoverParams;

/**
 * Filter stop words in a document.
 */
public final class StopWordsRemoverStreamOp extends MapStreamOp <StopWordsRemoverStreamOp>
	implements StopWordsRemoverParams <StopWordsRemoverStreamOp> {

	public StopWordsRemoverStreamOp() {
		this(null);
	}

	public StopWordsRemoverStreamOp(Params params) {
		super(StopWordsRemoverMapper::new, params);
	}
}
