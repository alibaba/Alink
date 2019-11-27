package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.ImputerModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.dataproc.ImputerPredictParams;

/**
 * Imputer completes missing values in a dataSet, but only same type of columns can be selected at the same time.
 * Imputer Predict completes missing values in a dataSet with model which trained from Inputer train.
 * Strategy support min, max, mean or value.
 * If min, will replace missing value with min of the column.
 * If max, will replace missing value with max of the column.
 * If mean, will replace missing value with mean of the column.
 * If value, will replace missing value with the value.
 */
public class ImputerPredictBatchOp extends ModelMapBatchOp <ImputerPredictBatchOp>
	implements ImputerPredictParams <ImputerPredictBatchOp> {

	public ImputerPredictBatchOp() {
		this(null);
	}

	public ImputerPredictBatchOp(Params params) {
		super(ImputerModelMapper::new, params);
	}
}




