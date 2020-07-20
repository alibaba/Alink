package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.FmClassifierTrainBatchOp;

import com.alibaba.alink.params.recommendation.FmPredictParams;
import com.alibaba.alink.params.recommendation.FmTrainParams;
import com.alibaba.alink.pipeline.Trainer;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Fm classifier pipeline op.
 *
 */
public class FmClassifier extends Trainer <FmClassifier, FmModel>
	implements FmTrainParams<FmClassifier>, FmPredictParams<FmClassifier>, HasLazyPrintModelInfo<FmClassifier> {

	private static final long serialVersionUID = 1557009335800161587L;

	public FmClassifier() {
		super();
	}

	public FmClassifier(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new FmClassifierTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
