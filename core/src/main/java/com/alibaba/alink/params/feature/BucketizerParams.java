package com.alibaba.alink.params.feature;

import org.apache.flink.calcite.shaded.com.google.common.primitives.Ints;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

/**
 * Params for bucketizer.
 */
public interface BucketizerParams<T> extends
	QuantileDiscretizerPredictParams<T>,
	HasLeftOpen<T> {

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

	default T setCutsArray(double[] values, int[] cutsLength){
		Preconditions.checkState(values.length == Arrays.stream(cutsLength).sum(), "Input format error!");
		double[][] cutsArray = new double[cutsLength.length][];
		int start = 0;
		for(int i = 0; i < cutsLength.length; i++){
			cutsArray[i] = new double[cutsLength[i]];
			System.arraycopy(values, start, cutsArray[i], 0, cutsLength[i]);
			start += cutsLength[i];
		}
		return set(CUTS_ARRAY, cutsArray);
	}

}
