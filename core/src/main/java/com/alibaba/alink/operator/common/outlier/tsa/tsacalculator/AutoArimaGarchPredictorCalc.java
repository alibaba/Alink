package com.alibaba.alink.operator.common.outlier.tsa.tsacalculator;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.timeseries.arimagarch.ArimaGarch;
import com.alibaba.alink.operator.common.timeseries.arimagarch.ArimaGarchModel;
import com.alibaba.alink.operator.common.timeseries.arimagarch.ModelInfo;
import com.alibaba.alink.params.outlier.tsa.baseparams.BaseStreamPredictParams;
import com.alibaba.alink.params.timeseries.AutoArimaGarchParams;
import com.alibaba.alink.params.timeseries.HasArimaGarchMethod;
import com.alibaba.alink.params.timeseries.HasIcType;

import java.util.ArrayList;

public class AutoArimaGarchPredictorCalc extends TimeSeriesPredictorCalc {
	private static final long serialVersionUID = 2069937892958311944L;
	HasIcType.IcType ic;
	boolean ifGARCH11;
	int maxARIMA;
	int maxGARCH;
	HasArimaGarchMethod.ArimaGarchMethod arimaGarchMethod;

	AutoArimaGarchPredictorCalc() {}

	public AutoArimaGarchPredictorCalc(Params params) {
		ic = params.get(AutoArimaGarchParams.IC_TYPE);
		maxARIMA = params.get(AutoArimaGarchParams.MAX_ARIMA);
		maxGARCH = params.get(AutoArimaGarchParams.MAX_GARCH);
		predictNum = params.get(BaseStreamPredictParams.PREDICT_NUM);
		ifGARCH11 = params.get(AutoArimaGarchParams.IF_GARCH11);
		arimaGarchMethod = params.get(AutoArimaGarchParams.ARIMA_GARCH_METHOD);
	}

	@Override
	public double[] forecastWithoutException(double[] data, int forecastStep, boolean trainBeforeForecast) {
		ArimaGarchModel aag = ArimaGarch.autoFit(data, ic, arimaGarchMethod, maxARIMA, maxGARCH, ifGARCH11);
		ArrayList <double[]> forecast = aag.forecast(forecastStep);
		return forecast.get(0);
	}

	@Override
	public double[] predict(double[] data) {
		ArimaGarchModel aag = ArimaGarch.autoFit(data, ic, arimaGarchMethod, maxARIMA, maxGARCH, ifGARCH11);

		if (aag.isGoodFit()) {
			ArrayList <double[]> forecast = aag.forecast(this.predictNum);
			return forecast.get(0);
		} else {
			return null;
		}
	}

	@Override
	public Row map(Row in) {
		double[] data = (double[]) in.getField(groupNumber + 1);

		ArimaGarchModel aag = ArimaGarch.autoFit(data, ic, arimaGarchMethod, maxARIMA, maxGARCH, ifGARCH11);

		if (aag.isGoodFit()) {
			ArrayList <double[]> forecast = aag.forecast(this.predictNum);
			ModelInfo mi = aag.mi;
			return getData(
				in,
				new DenseVector(forecast.get(0)),
				new DenseVector(forecast.get(1)),
				new DenseVector(forecast.get(2)),
				new DenseVector(forecast.get(3)),
				JsonConverter.toJson(mi.order),
				mi.ic,
				mi.loglike,
				new DenseVector(mi.arCoef),
				new DenseVector(mi.seARCoef),
				new DenseVector(mi.maCoef),
				new DenseVector(mi.seMACoef),
				mi.intercept,
				mi.seIntercept,
				new DenseVector(mi.alpha),
				new DenseVector(mi.seAlpha),
				new DenseVector(mi.beta),
				new DenseVector(mi.seBeta),
				mi.c,
				mi.seC,
				new DenseVector(mi.estResidual),
				new DenseVector(mi.hHat),
				mi.ifHetero
			);

		} else {
			return getData(in,
				null, null, null, null,
				null, null, null, null,
				null, null, null, null,
				null, null, null, null,
				null, null, null, null,
				null, null);
		}
	}

	@Override
	public AutoArimaGarchPredictorCalc clone() {
		AutoArimaGarchPredictorCalc calc = new AutoArimaGarchPredictorCalc();
		calc.ic = ic;
		calc.ifGARCH11 = ifGARCH11;
		calc.maxARIMA = maxARIMA;
		calc.maxGARCH = maxGARCH;
		calc.arimaGarchMethod = arimaGarchMethod;
		calc.predictNum = predictNum;
		calc.groupNumber = groupNumber;
		return calc;
	}

}
