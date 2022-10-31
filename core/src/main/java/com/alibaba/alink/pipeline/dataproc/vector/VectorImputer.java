package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorImputerTrainBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.dataproc.vector.VectorImputerTrainLocalOp;
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
@NameCn("向量缺失值填充")
public class VectorImputer extends Trainer <VectorImputer, VectorImputerModel> implements
	VectorImputerTrainParams <VectorImputer>,
	VectorImputerPredictParams <VectorImputer> {

	private static final long serialVersionUID = 3245982875941711639L;

	public VectorImputer() {
		super();
	}

	public VectorImputer(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new VectorImputerTrainBatchOp(params).linkFrom(in);
	}

	@Override
	protected LocalOperator <?> train(LocalOperator <?> in) {
		return new VectorImputerTrainLocalOp(params).linkFrom(in);
	}
}

