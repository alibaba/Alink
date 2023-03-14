package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorImputerModelMapper;
import com.alibaba.alink.params.dataproc.vector.VectorImputerPredictParams;

/**
 * Imputer completes missing values in a dataSet, but only same type of columns can be selected at the same time.
 * Imputer Train will train a model for predict.
 * Strategy support min, max, mean or value.
 * If min, will replace missing value with min of the column.
 * If max, will replace missing value with max of the column.
 * If mean, will replace missing value with mean of the column.
 * If value, will replace missing value with the value.
 */
@NameCn("向量缺失值填充预测")
@NameEn("Vector Imputer Prediction")
public class VectorImputerPredictBatchOp extends ModelMapBatchOp <VectorImputerPredictBatchOp>
	implements VectorImputerPredictParams <VectorImputerPredictBatchOp> {

	private static final long serialVersionUID = 5319255930355295245L;

	public VectorImputerPredictBatchOp() {
		this(null);
	}

	public VectorImputerPredictBatchOp(Params params) {
		super(VectorImputerModelMapper::new, params);
	}
}




