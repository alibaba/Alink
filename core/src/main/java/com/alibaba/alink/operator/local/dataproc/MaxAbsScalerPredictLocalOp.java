package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.MaxAbsScalerModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.dataproc.MaxAbsScalerPredictParams;

/**
 * MaxAbsScaler transforms a dataSet of rows,rescaling each feature to range
 * [-1, 1] by dividing through the maximum absolute value in each feature.
 * MaxAbsPredict will scale the dataSet with model which trained from MaxAbsTrain.
 */
@NameCn("绝对值最大化批预测")
public final class MaxAbsScalerPredictLocalOp extends ModelMapLocalOp <MaxAbsScalerPredictLocalOp>
	implements MaxAbsScalerPredictParams <MaxAbsScalerPredictLocalOp> {

	public MaxAbsScalerPredictLocalOp() {
		this(new Params());
	}

	public MaxAbsScalerPredictLocalOp(Params params) {
		super(MaxAbsScalerModelMapper::new, params);
	}

}
