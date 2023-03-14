package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.StandardScalerModelMapper;
import com.alibaba.alink.operator.common.linear.SoftmaxModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.StandardPredictParams;

/**
 * StandardScaler transforms a dataset, normalizing each feature to have unit standard deviation and/or zero mean.
 */
@NameCn("标准化流预测")
@NameEn("Standard Scaler Prediction")
public class StandardScalerPredictStreamOp extends ModelMapStreamOp <StandardScalerPredictStreamOp>
	implements StandardPredictParams <StandardScalerPredictStreamOp> {

	private static final long serialVersionUID = -24812532674204029L;

	public StandardScalerPredictStreamOp() {
		super(StandardScalerModelMapper::new, new Params());
	}

	public StandardScalerPredictStreamOp(Params params) {
		super(StandardScalerModelMapper::new, params);
	}

	public StandardScalerPredictStreamOp(BatchOperator srt) {
		this(srt, new Params());
	}

	public StandardScalerPredictStreamOp(BatchOperator srt, Params params) {
		super(srt, StandardScalerModelMapper::new, params);
	}

}
