package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMaxAbsScalerModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorMaxAbsScalerPredictParams;

/**
 * MaxAbsScaler transforms a dataset of Vector rows,rescaling each feature to range
 * [-1, 1] by dividing through the maximum absolute value in each feature.
 * MaxAbsPredict will scale the dataset with model which trained from MaxAbsTrain.
 */
@NameCn("向量绝对值最大化预测")
public class VectorMaxAbsScalerPredictStreamOp extends ModelMapStreamOp <VectorMaxAbsScalerPredictStreamOp>
	implements VectorMaxAbsScalerPredictParams <VectorMaxAbsScalerPredictStreamOp> {

	private static final long serialVersionUID = 1839539414612336143L;

	public VectorMaxAbsScalerPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public VectorMaxAbsScalerPredictStreamOp(BatchOperator model, Params params) {
		super(model, VectorMaxAbsScalerModelMapper::new, params);
	}

}
