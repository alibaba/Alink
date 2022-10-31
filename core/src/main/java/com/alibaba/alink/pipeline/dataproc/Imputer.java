package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ImputerTrainBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.dataproc.ImputerTrainLocalOp;
import com.alibaba.alink.params.dataproc.ImputerPredictParams;
import com.alibaba.alink.params.dataproc.ImputerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Imputer completes missing values in a dataset, but only same type of columns can be selected at the same time.
 *
 * Strategy support min, max, mean or value.
 * If min, will replace missing value with min of the column.
 * If max, will replace missing value with max of the column.
 * If mean, will replace missing value with mean of the column.
 * If value, will replace missing value with the value.
 */
@NameCn("缺失值填充")
public class Imputer extends Trainer <Imputer, ImputerModel> implements
	ImputerTrainParams <Imputer>,
	ImputerPredictParams <Imputer>,
	HasLazyPrintModelInfo<Imputer> {

	private static final long serialVersionUID = 2312681208119505001L;

	public Imputer() {
		super();
	}

	public Imputer(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new ImputerTrainBatchOp(params).linkFrom(in);
	}

	@Override
	protected LocalOperator <?> train(LocalOperator <?> in) {
		return new ImputerTrainLocalOp(params).linkFrom(in);
	}

}

