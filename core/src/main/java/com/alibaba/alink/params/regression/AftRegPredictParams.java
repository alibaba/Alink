package com.alibaba.alink.params.regression;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

/**
 * Params for AftRegressionPredictor.
 */
public interface AftRegPredictParams<T> extends
	RegPredictParams <T>,
	HasVectorColDefaultAsNull <T>,
	HasPredictionDetailCol <T> {

	@NameCn("分位数概率数组")
	@DescCn("分位数概率数组")
	ParamInfo <double[]> QUANTILE_PROBABILITIES = ParamInfoFactory
		.createParamInfo("quantileProbabilities", double[].class)
		.setDescription("Array of quantile probabilities.")
		.setHasDefaultValue(new double[] {0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99})
		.build();

	default double[] getQuantileProbabilities() {
		return get(QUANTILE_PROBABILITIES);
	}

	default T setQuantileProbabilities(double[] value) {
		return set(QUANTILE_PROBABILITIES, value);
	}

}
