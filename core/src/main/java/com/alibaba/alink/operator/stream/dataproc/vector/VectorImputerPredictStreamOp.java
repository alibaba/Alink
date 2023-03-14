package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.vector.VectorImputerModelMapper;
import com.alibaba.alink.operator.common.tree.predictors.TreeModelEncoderModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorImputerPredictParams;

/**
 * Imputer completes missing values in a dataset, but only same type of columns can be selected at the same time.
 * Imputer Predict completes missing values in a dataset with model which trained from Inputer train.
 * Strategy support min, max, mean or value.
 * If min, will replace missing value with min of the column.
 * If max, will replace missing value with max of the column.
 * If mean, will replace missing value with mean of the column.
 * If value, will replace missing value with the value.
 */
@NameCn("向量缺失值填充预测")
@NameEn("Vector imputer prediction")
public class VectorImputerPredictStreamOp extends ModelMapStreamOp <VectorImputerPredictStreamOp>
	implements VectorImputerPredictParams <VectorImputerPredictStreamOp> {

	private static final long serialVersionUID = 6941917975118911308L;

	public VectorImputerPredictStreamOp() {
		super(VectorImputerModelMapper::new, new Params());
	}

	public VectorImputerPredictStreamOp(Params params) {
		super(VectorImputerModelMapper::new, params);
	}

	public VectorImputerPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public VectorImputerPredictStreamOp(BatchOperator model, Params params) {
		super(model, VectorImputerModelMapper::new, params);
	}

}
