package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.MaxAbsScalerModelMapper;
import com.alibaba.alink.operator.common.timeseries.LSTNetModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.MaxAbsScalerPredictParams;

/**
 * MaxAbsScaler transforms a dataset of Vector rows,rescaling each feature to range
 * [-1, 1] by dividing through the maximum absolute value in each feature.
 * MaxAbsPredict will scale the dataset with model which trained from MaxAbsTrain.
 */
@NameCn("绝对值最大化流预测")
@NameEn("Max Abs Scaler Prediction")
public class MaxAbsScalerPredictStreamOp extends ModelMapStreamOp <MaxAbsScalerPredictStreamOp>
	implements MaxAbsScalerPredictParams <MaxAbsScalerPredictStreamOp> {

	private static final long serialVersionUID = -766923466534304512L;

	public MaxAbsScalerPredictStreamOp() {
		super(MaxAbsScalerModelMapper::new, new Params());
	}

	public MaxAbsScalerPredictStreamOp(Params params) {
		super(MaxAbsScalerModelMapper::new, params);
	}

	public MaxAbsScalerPredictStreamOp(BatchOperator srt) {
		this(srt, new Params());
	}

	public MaxAbsScalerPredictStreamOp(BatchOperator model, Params params) {
		super(model, MaxAbsScalerModelMapper::new, params);
	}

}
