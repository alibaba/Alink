package com.alibaba.alink.pipeline.dataproc.vector;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorImputerTrainBatchOp;
import com.alibaba.alink.params.dataproc.vector.VectorImputerPredictParams;
import com.alibaba.alink.params.dataproc.vector.VectorImputerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Imputer completes missing values in a data set, but only same type of columns can be selected at the same time.
 * <p>
 * Strategy support min, max, mean or value.
 * If min, will replace missing value with min of the column.
 * If max, will replace missing value with max of the column.
 * If mean, will replace missing value with mean of the column.
 * If value, will replace missing value with the value.
 */
public class VectorImputer extends Trainer <VectorImputer, VectorImputerModel> implements
	VectorImputerTrainParams <VectorImputer>,
	VectorImputerPredictParams <VectorImputer> {

	public VectorImputer() {
		super();
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new VectorImputerTrainBatchOp(params).linkFrom(in);
	}
}

