package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.MaxAbsScalerTrainBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.dataproc.MaxAbsScalerTrainLocalOp;
import com.alibaba.alink.params.dataproc.MaxAbsScalerPredictParams;
import com.alibaba.alink.params.dataproc.MaxAbsScalerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * MaxAbsScaler transforms a dataset of Vector rows,rescaling each feature to range
 * [-1, 1] by dividing through the maximum absolute value in each feature.
 */
@NameCn("绝对值最大化")
public class MaxAbsScaler extends Trainer <MaxAbsScaler, MaxAbsScalerModel> implements
	MaxAbsScalerTrainParams <MaxAbsScaler>,
	MaxAbsScalerPredictParams <MaxAbsScaler>,
	HasLazyPrintModelInfo <MaxAbsScaler> {

	private static final long serialVersionUID = 2515889163832988532L;

	public MaxAbsScaler() {
		super();
	}

	public MaxAbsScaler(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new MaxAbsScalerTrainBatchOp(this.getParams()).linkFrom(in);
	}

	@Override
	protected LocalOperator <?> train(LocalOperator <?> in) {
		return new MaxAbsScalerTrainLocalOp(this.getParams()).linkFrom(in);
	}
}

