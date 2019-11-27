package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * Regression evaluation metrics.
 */
public final class RegressionMetrics extends BaseSimpleRegressionMetrics<RegressionMetrics> {
	public static final ParamInfo <Double> SSE = ParamInfoFactory
		.createParamInfo("SSE", Double.class)
		.setDescription("Sum of Squares for Error, SSE = sum(yi-fi)^2")
		.setRequired()
		.build();
	public static final ParamInfo <Double> SST = ParamInfoFactory
		.createParamInfo("SST", Double.class)
		.setDescription("Sum of Squared for Total, SST = sum(yi-y_hat)^2")
		.setRequired()
		.build();
	public static final ParamInfo <Double> SSR = ParamInfoFactory
		.createParamInfo("SSR", Double.class)
		.setDescription("Sum of Squares for Regression, SSR = sum(fi_y_hat)^2")
		.setRequired()
		.build();
	public static final ParamInfo <Double> R2 = ParamInfoFactory
		.createParamInfo("R2", Double.class)
		.setDescription("Coefficient of Determination, R2 = 1 - SSE/SST")
		.setRequired()
		.build();
	public static final ParamInfo <Double> R = ParamInfoFactory
		.createParamInfo("R", Double.class)
		.setDescription("Multiple CorrelationBak Coeffient, R = sqrt(R2)")
		.setRequired()
		.build();
	public static final ParamInfo <Double> SAE = ParamInfoFactory
		.createParamInfo("SAE", Double.class)
		.setDescription("Sum of Absolute Error/Difference, SAE = sum|fi-yi|")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MAPE = ParamInfoFactory
		.createParamInfo("MAPE", Double.class)
		.setDescription(" Mean Absolute Percentage Error, MAPE = sum|(fi-yi)/yi|*100/N")
		.setRequired()
		.build();
	public static final ParamInfo <Double> COUNT = ParamInfoFactory
		.createParamInfo("count", Double.class)
		.setDescription("count")
		.setRequired()
		.build();
	public static final ParamInfo <Double> Y_MEAN = ParamInfoFactory
		.createParamInfo("yMean", Double.class)
		.setDescription("yMean")
		.setRequired()
		.build();
	public static final ParamInfo <Double> PREDICTION_MEAN = ParamInfoFactory
		.createParamInfo("predictionMean", Double.class)
		.setDescription("predictionMean")
		.setRequired()
		.build();

	public Double getSse() {return get(SSE);}

	public Double getSst() {return get(SST);}

	public Double getSsr() {return get(SSR);}

	public Double getR2() {return get(R2);}

	public Double getR() {return get(R);}

	public Double getSae() {return get(SAE);}

	public Double getMape() {return get(MAPE);}

	public Double getCount() {return get(COUNT);}

	public Double getYMean() {return get(Y_MEAN);}

	public Double getPredictionMean() {return get(PREDICTION_MEAN);}

	public RegressionMetrics(Row row) {
		super(row);
	}

	public RegressionMetrics(Params params){
		super(params);
	}
}
