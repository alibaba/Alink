package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ImputerTrainBatchOp;
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
public class Imputer extends Trainer <Imputer, ImputerModel> implements
	ImputerTrainParams <Imputer>,
	ImputerPredictParams <Imputer> {

	public Imputer() {
		super();
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new ImputerTrainBatchOp(params).linkFrom(in);
	}

}

