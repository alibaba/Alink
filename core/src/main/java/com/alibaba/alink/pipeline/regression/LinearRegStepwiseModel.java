package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.params.regression.LinearRegStepwisePredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * @author weibo zhao
 */
@NameCn("线性回归Stepwise模型")
public class LinearRegStepwiseModel extends MapModel <LinearRegStepwiseModel>
	implements LinearRegStepwisePredictParams <LinearRegStepwiseModel> {
	private static final long serialVersionUID = -3128465271424347623L;

	public LinearRegStepwiseModel() {
		this(null);
	}

	public LinearRegStepwiseModel(Params params) {
		super(LinearModelMapper::new, params);
	}
}
