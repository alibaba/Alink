package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.StopWordsRemoverMapper;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.params.nlp.StopWordsRemoverParams;

/**
 * Filter stop words in a document.
 */
public final class StopWordsRemoverBatchOp extends MapBatchOp <StopWordsRemoverBatchOp>
	implements StopWordsRemoverParams <StopWordsRemoverBatchOp> {

	public StopWordsRemoverBatchOp() {
		this(new Params());
	}

	public StopWordsRemoverBatchOp(Params params) {
		super(StopWordsRemoverMapper::new, params);
	}
}
