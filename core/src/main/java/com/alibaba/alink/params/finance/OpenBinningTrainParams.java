package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface OpenBinningTrainParams<T> extends
	BinningTrainParams <T> {

	ParamInfo <double[][]> SPLITS_ARRAYS = ParamInfoFactory
		.createParamInfo("splitsArrays", double[][].class)
		.setDescription("splitsArrays")
		.setHasDefaultValue(null)
		.build();

	default double[][] getSplitsArrays() {
		return get(SPLITS_ARRAYS);
	}

	default T setSplitsArrays(double[][] value) {
		return set(SPLITS_ARRAYS, value);
	}

	ParamInfo <String[][]> LABELS_ARRAYS = ParamInfoFactory
		.createParamInfo("labelsArrays", String[][].class)
		.setDescription("splitsArrays")
		.setHasDefaultValue(null)
		.build();

	default String[][] getLabelsArrays() {
		return get(LABELS_ARRAYS);
	}

	default T setLabelsArrays(String[][] value) {
		return set(LABELS_ARRAYS, value);
	}
}
