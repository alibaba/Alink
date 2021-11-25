package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * Params for bucketizer.
 */
public interface BucketizerParams<T> extends
	QuantileDiscretizerPredictParams <T>,
	HasLeftOpen <T> {

	/**
	 * @cn-name 多列的切分点
	 * @cn 多列的切分点
	 */

	ParamInfo <double[][]> CUTS_ARRAY = ParamInfoFactory
		.createParamInfo("cutsArray", double[][].class)
		.setDescription("Cut points array, each of them is used for the corresponding selected column.")
		.build();

	default double[][] getCutsArray() {
		return get(CUTS_ARRAY);
	}

	default T setCutsArray(double[]... value) {
		return set(CUTS_ARRAY, value);
	}

	ParamInfo <String[]> CUTS_ARRAY_STR = ParamInfoFactory
		.createParamInfo("cutsArrayStr", String[].class)
		.setDescription("Cut points array, each of them is used for the corresponding selected column.")
		.build();

}
