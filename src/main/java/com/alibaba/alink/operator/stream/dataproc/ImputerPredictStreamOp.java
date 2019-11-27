package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.ImputerModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.ImputerPredictParams;

/**
 * Imputer completes missing values in a dataset, but only same type of columns can be selected at the same time.
 * Imputer Predict completes missing values in a dataset with model which trained from Inputer train.
 * Strategy support min, max, mean or value.
 * If min, will replace missing value with min of the column.
 * If max, will replace missing value with max of the column.
 * If mean, will replace missing value with mean of the column.
 * If value, will replace missing value with the value.
 */
public class ImputerPredictStreamOp extends ModelMapStreamOp <ImputerPredictStreamOp>
	implements ImputerPredictParams <ImputerPredictStreamOp> {

	public ImputerPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public ImputerPredictStreamOp(BatchOperator model, Params params) {
		super(model, ImputerModelMapper::new, params);
	}

}
