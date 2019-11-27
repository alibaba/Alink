package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.Word2VecModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.nlp.Word2VecPredictParams;

public class Word2VecPredictBatchOp extends ModelMapBatchOp <Word2VecPredictBatchOp>
	implements Word2VecPredictParams <Word2VecPredictBatchOp> {
	public Word2VecPredictBatchOp() {
		this(null);
	}

	public Word2VecPredictBatchOp(Params params) {
		super(Word2VecModelMapper::new, params);
	}
}
