package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * Params for bucketizer.
 */
public interface BucketizerParams<T> extends
	QuantileDiscretizerPredictParams<T>,
	HasLeftOpen<T> {

	ParamInfo <double[][]> CUTS_ARRAY = ParamInfoFactory
		.createParamInfo("cuts", double[][].class)
		.setDescription("Cut points array, each of them is used for the corresponding selected column.")
		.build();

	default double[][] getCutsArray() {
		return get(CUTS_ARRAY);
	}

	default T setCutsArray(double[]... value) {
		return set(CUTS_ARRAY, value);
	}

}
