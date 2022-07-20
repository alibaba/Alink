package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.timeseries.sarima.Sarima;
import com.alibaba.alink.operator.common.timeseries.sarima.SarimaModel;
import com.alibaba.alink.params.timeseries.ArimaParams;
import com.alibaba.alink.params.timeseries.HasEstmateMethod;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;

public class ArimaMapper extends TimeSeriesSingleMapper {

	private HasEstmateMethod.EstMethod estMethod;
	private int p;
	private int d;
	private int q;
	private int sP;
	private int sD;
	private int sQ;
	private int seasonalPeriod;
	private int ifIntercept;

	public ArimaMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.estMethod = params.get(ArimaParams.EST_METHOD);
		Integer[] order = params.get(ArimaParams.ORDER);
		if (order.length != 3) {
			throw new AkIllegalOperatorParameterException("Order must has p, d and q.");
		}
		this.p = order[0];
		this.d = order[1];
		this.q = order[2];

		int[] seasonalOrder = params.get(ArimaParams.SEASONAL_ORDER);
		if (null == seasonalOrder) {
			this.seasonalPeriod = 1;
			this.sP = -1;
			this.sD = -1;
			this.sQ = -1;
		} else {
			this.seasonalPeriod = params.get(ArimaParams.SEASONAL_PERIOD);
			this.sP = seasonalOrder[0];
			this.sD = seasonalOrder[1];
			this.sQ = seasonalOrder[2];
		}

		this.ifIntercept = d > 0 ? 0 : 1;
	}

	@Override
	protected Tuple2 <double[], String> predictSingleVar(Timestamp[] historyTimes,
														 double[] historyVals,
														 int predictNum) {
		if (historyVals == null || historyVals.length == 0) {
			return Tuple2.of(null, null);
		}

		boolean isEqualSeries = true;
		double curVal = historyVals[0];
		for (int i = 1; i < historyVals.length; i++) {
			if (curVal != historyVals[i]) {
				isEqualSeries = false;
				break;
			}
		}

		if (isEqualSeries) {
			double[] predictValues = new double[predictNum];
			Arrays.fill(predictValues, historyVals[historyVals.length - 1]);
			return Tuple2.of(predictValues, null);
		}

		SarimaModel model = Sarima.fit(historyVals, p, d, q, sP, sD, sQ,
			estMethod, ifIntercept, seasonalPeriod);

		ArrayList <double[]> forecast = model.forecast(predictNum);

		return toArimaInfo(model, forecast);
	}

	static Tuple2 <double[], String> toArimaInfo(SarimaModel model, ArrayList <double[]> forecast) {
		ArimaInfo arimaInfo = new ArimaInfo();
		arimaInfo.forecast = forecast.get(0);
		arimaInfo.stdError = forecast.get(1);
		arimaInfo.lower = forecast.get(2);
		arimaInfo.upper = forecast.get(3);
		arimaInfo.arCoef = model.arima.arma.estimate.arCoef;
		arimaInfo.arCoefStdError = model.arima.arma.estimate.arCoefStdError;
		arimaInfo.maCoef = model.arima.arma.estimate.maCoef;
		arimaInfo.maCoefStdError = model.arima.arma.estimate.maCoefStdError;
		arimaInfo.intercept = model.arima.arma.estimate.intercept;
		arimaInfo.interceptStdError = model.arima.arma.estimate.interceptStdError;
		arimaInfo.variance = model.arima.arma.estimate.variance;
		arimaInfo.varianceStdError = model.arima.arma.estimate.varianceStdError;
		arimaInfo.logLikelihood = model.arima.arma.estimate.logLikelihood;
		arimaInfo.iC = model.ic;
		arimaInfo.seasonalArCoef = model.sARCoef;
		arimaInfo.seasonalArCoefStdError = model.sArStdError;
		arimaInfo.seasonalMaCoef = model.sMACoef;
		arimaInfo.seasonalMaCoefStdError = model.sMaStdError;

		arimaInfo.p = model.arima.p;
		arimaInfo.d = model.arima.d;
		arimaInfo.q = model.arima.q;

		arimaInfo.sP = model.sP;
		arimaInfo.sD = model.sD;
		arimaInfo.sQ = model.sQ;

		arimaInfo.seasonalPeriod = model.seasonalPeriod;

		return Tuple2.of(forecast.get(0), JsonConverter.toJson(arimaInfo));
	}

	@Override
	protected String getPredictionDetails(String[] singelPredictionDetails) {
		if (singelPredictionDetails == null) {
			return null;
		}
		ArimaInfo[] arimaInfos = new ArimaInfo[singelPredictionDetails.length];
		for (int i = 0; i < singelPredictionDetails.length; i++) {
			if (singelPredictionDetails[i] != null) {
				arimaInfos[i] = JsonConverter.fromJson(singelPredictionDetails[i], ArimaInfo.class);
			}
		}
		return JsonConverter.toJson(arimaInfos);
	}

	private static class ArimaInfo {
		int p;
		int d;
		int q;

		int sP;
		int sD;
		int sQ;

		int seasonalPeriod;

		double[] forecast;
		double[] lower;
		double[] upper;
		double[] stdError;

		double[] arCoef;
		double[] arCoefStdError;
		double[] maCoef;
		double[] maCoefStdError;

		double[] seasonalArCoef;
		double[] seasonalArCoefStdError;
		double[] seasonalMaCoef;
		double[] seasonalMaCoefStdError;

		double intercept;
		double interceptStdError;
		double variance;
		double varianceStdError;
		double iC;
		double logLikelihood;
	}
}
