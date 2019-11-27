package com.alibaba.alink.pipeline.clustering;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.LdaTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.clustering.LdaPredictParams;
import com.alibaba.alink.params.clustering.LdaTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
 */
public class Lda extends Trainer <Lda, LdaModel> implements
	LdaTrainParams <Lda>,
	LdaPredictParams <Lda> {

	public Lda() {
		super();
	}

	public Lda(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new LdaTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
