package com.alibaba.alink.pipeline.nlp;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.Word2VecTrainBatchOp;
import com.alibaba.alink.params.nlp.Word2VecPredictParams;
import com.alibaba.alink.params.nlp.Word2VecTrainParams;
import com.alibaba.alink.pipeline.Trainer;

public class Word2Vec extends Trainer <Word2Vec, Word2VecModel>
	implements Word2VecTrainParams <Word2Vec>, Word2VecPredictParams <Word2Vec> {
	@Override
	protected BatchOperator train(BatchOperator in) {
		return new Word2VecTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
