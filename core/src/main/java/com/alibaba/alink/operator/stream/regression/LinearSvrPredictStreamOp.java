package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.LinearSvrPredictParams;

/**
 * *
 *
 * @author weibo zhao
 */
@NameCn("线性SVR流预测")
@NameEn("Linear SVR Prediction")
public class LinearSvrPredictStreamOp extends ModelMapStreamOp <LinearSvrPredictStreamOp>
	implements LinearSvrPredictParams <LinearSvrPredictStreamOp> {

	private static final long serialVersionUID = 5652354975643756620L;

	public LinearSvrPredictStreamOp() {
		super(LinearModelMapper::new, new Params());
	}

	public LinearSvrPredictStreamOp(Params params) {
		super(LinearModelMapper::new, params);
	}

	public LinearSvrPredictStreamOp(BatchOperator model) {
		super(model, LinearModelMapper::new, new Params());
	}
	
	public LinearSvrPredictStreamOp(BatchOperator model, Params params) {
		super(model, LinearModelMapper::new, params);
	}

}
