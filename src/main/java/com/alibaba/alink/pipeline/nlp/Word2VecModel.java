package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.common.nlp.Word2VecModelMapper;
import com.alibaba.alink.params.nlp.Word2VecPredictParams;
import com.alibaba.alink.pipeline.MapModel;

public class Word2VecModel extends MapModel<Word2VecModel>
	implements Word2VecPredictParams <Word2VecModel> {
	public Word2VecModel(Params params) {
		super(Word2VecModelMapper::new, params);
	}

	public AlgoOperator getVectors() {
		return BatchOperator.fromTable(this.getModelData()).setMLEnvironmentId(getMLEnvironmentId());
	}
}
