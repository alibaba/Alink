package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.StandardScalerTrainBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.dataproc.StandardScalerTrainLocalOp;
import com.alibaba.alink.params.dataproc.StandardPredictParams;
import com.alibaba.alink.params.dataproc.StandardTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * StandardScaler transforms a dataset, normalizing each feature to have unit standard deviation and/or zero mean.
 */
@NameCn("标准化")
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

	@Override
	protected LocalOperator <?> train(LocalOperator <?> in) {
		return new StandardScalerTrainLocalOp(this.getParams()).linkFrom(in);
	}
}

