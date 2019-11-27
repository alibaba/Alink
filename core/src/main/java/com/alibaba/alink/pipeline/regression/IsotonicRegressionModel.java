package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.operator.common.regression.IsotonicRegressionModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Isotonic Regression.
 * Implement parallelized pool adjacent violators algorithm.
 * Support single feature input or vector input(extractor one index of the vector).
 */
public class IsotonicRegressionModel extends MapModel<IsotonicRegressionModel> {

	public IsotonicRegressionModel() {this(null);}

	public IsotonicRegressionModel(Params params) {
		super(IsotonicRegressionModelMapper::new, params);
	}

}
