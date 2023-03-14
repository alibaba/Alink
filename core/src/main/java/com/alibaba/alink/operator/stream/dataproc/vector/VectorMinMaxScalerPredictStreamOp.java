package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMaxAbsScalerModelMapper;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMinMaxScalerModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorMinMaxScalerPredictParams;

/**
 * MinMaxScaler transforms a dataset of Vector rows, rescaling each feature
 * to a specific range [min, max). (often [0, 1]).
 * MinMaxScalerPredict will scale the dataset with model which trained from MaxAbsTrain.
 */
@NameCn("向量归一化预测")
@NameEn("Vector min-max scaler prediction")
public class VectorMinMaxScalerPredictStreamOp extends ModelMapStreamOp <VectorMinMaxScalerPredictStreamOp>
	implements VectorMinMaxScalerPredictParams <VectorMinMaxScalerPredictStreamOp> {

	private static final long serialVersionUID = -4616069594976834612L;

	public VectorMinMaxScalerPredictStreamOp() {
		super(VectorMinMaxScalerModelMapper::new, new Params());
	}

	public VectorMinMaxScalerPredictStreamOp(Params params) {
		super(VectorMinMaxScalerModelMapper::new, params);
	}

	public VectorMinMaxScalerPredictStreamOp(BatchOperator srt) {
		this(srt, new Params());
	}

	public VectorMinMaxScalerPredictStreamOp(BatchOperator srt, Params params) {
		super(srt, VectorMinMaxScalerModelMapper::new, params);
	}

}
