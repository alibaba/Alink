package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.StandardScalerTrainBatchOp;
import com.alibaba.alink.params.dataproc.StandardPredictParams;
import com.alibaba.alink.params.dataproc.StandardTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * StandardScaler transforms a dataset, normalizing each feature to have unit standard deviation and/or zero mean.
 */
public class StandardScaler extends Trainer <StandardScaler, StandardScalerModel> implements
	StandardTrainParams <StandardScaler>,
	StandardPredictParams <StandardScaler>,
	HasLazyPrintModelInfo <StandardScaler> {

	private static final long serialVersionUID = -2881876388698556871L;

	public StandardScaler() {
		super();
	}

	public StandardScaler(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new StandardScalerTrainBatchOp(this.getParams()).linkFrom(in);
	}
}

