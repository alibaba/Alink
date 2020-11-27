package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.FmRegressorTrainBatchOp;
import com.alibaba.alink.params.recommendation.FmPredictParams;
import com.alibaba.alink.params.recommendation.FmTrainParams;
import com.alibaba.alink.pipeline.Trainer;
import com.alibaba.alink.pipeline.classification.FmModel;

/**
 * Fm regression pipeline op.
 */
public class FmRegressor extends Trainer <FmRegressor, FmModel>
	implements FmTrainParams <FmRegressor>, FmPredictParams <FmRegressor>, HasLazyPrintModelInfo <FmRegressor>,
	HasLazyPrintTrainInfo <FmRegressor> {

	private static final long serialVersionUID = 3075669888794004056L;

	public FmRegressor() {
		super();
	}

	public FmRegressor(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new FmRegressorTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
