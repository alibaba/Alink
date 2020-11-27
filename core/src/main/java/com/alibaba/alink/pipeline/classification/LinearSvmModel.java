package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.params.classification.LinearSvmPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Linear svm pipeline model.
 */
public class LinearSvmModel extends MapModel <LinearSvmModel>
	implements LinearSvmPredictParams <LinearSvmModel> {

	private static final long serialVersionUID = -435507602601170282L;

	public LinearSvmModel() {this(null);}

	public LinearSvmModel(Params params) {
		super(LinearModelMapper::new, params);
	}

}
