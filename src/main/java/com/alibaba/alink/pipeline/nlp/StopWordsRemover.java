package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.StopWordsRemoverMapper;
import com.alibaba.alink.params.nlp.StopWordsRemoverParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Filter stop words in a document.
 */
public class StopWordsRemover extends MapTransformer<StopWordsRemover>
	implements StopWordsRemoverParams<StopWordsRemover> {

	public StopWordsRemover() {
		this(null);
	}

	public StopWordsRemover(Params params) {
		super(StopWordsRemoverMapper::new, params);
	}
}
