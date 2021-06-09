package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.recommendation.FmPredictParams;
import com.alibaba.alink.pipeline.classification.FmModel;

/**
 * Fm pipeline model.
 */
public class FmRegressionModel extends FmModel <FmRegressionModel>
	implements FmPredictParams <FmRegressionModel> {
	private static final long serialVersionUID = 8702278778833625190L;

	public FmRegressionModel() {this(null);}

	public FmRegressionModel(Params params) {
		super(params);
	}
}