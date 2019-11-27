package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.Word2VecModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.nlp.Word2VecPredictParams;

public class Word2VecPredictStreamOp extends ModelMapStreamOp <Word2VecPredictStreamOp>
	implements Word2VecPredictParams <Word2VecPredictStreamOp> {
	public Word2VecPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public Word2VecPredictStreamOp(BatchOperator model, Params params) {
		super(model, Word2VecModelMapper::new, params);
	}
}
