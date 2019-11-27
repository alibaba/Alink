package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * Base evaluation metrics for regression evaluation.
 */
public abstract class BaseSimpleRegressionMetrics<T extends BaseMetrics<T>> extends BaseMetrics<T> {
	public static final ParamInfo <Double> MSE = ParamInfoFactory
		.createParamInfo("MSE", Double.class)
		.setDescription("Mean Squared Error, MSE = SSE/N")
		.setRequired()
		.build();

	public static final ParamInfo <Double> MAE = ParamInfoFactory
		.createParamInfo("MAE", Double.class)
		.setDescription("Mean Absolute Error/Difference, MAE = SAE/N")
		.setRequired()
		.build();

	public static final ParamInfo <Double> RMSE = ParamInfoFactory
		.createParamInfo("RMSE", Double.class)
		.setDescription("Root Mean Squared Error, RMSE = sqrt(MSE)")
		.setRequired()
		.build();

	public static final ParamInfo <Double> EXPLAINED_VARIANCE = ParamInfoFactory
		.createParamInfo("Explained Variance", Double.class)
		.setDescription("Explained Variance, SSR / N")
		.setRequired()
		.build();

	public Double getMse() {
		return get(MSE);
	}

	public Double getMae() {
		return get(MAE);
	}

	public Double getRmse() {
		return get(RMSE);
	}

	public Double getExplainedVariance() {
		return get(EXPLAINED_VARIANCE);
	}

	public BaseSimpleRegressionMetrics(Row row) {
		super(row);
	}

	public BaseSimpleRegressionMetrics(Params params){
		super(params);
	}
}
