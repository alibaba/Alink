package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.regression.LinearRegStepwisePredictParams;
import com.alibaba.alink.params.regression.LinearRegStepwiseTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * @author weibo zhao
 */
@NameCn("线性回归Stepwise")
public class LinearRegStepwise extends Trainer <LinearRegStepwise, LinearRegStepwiseModel> implements
	LinearRegStepwiseTrainParams <LinearRegStepwise>,
	LinearRegStepwisePredictParams <LinearRegStepwise> {

	private static final long serialVersionUID = -3912478248752587259L;

	public LinearRegStepwise() {
		super(new Params());
	}

	public LinearRegStepwise(Params params) {
		super(params);
	}

}
