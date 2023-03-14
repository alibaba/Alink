package com.alibaba.alink.operator.common.outlier.tsa.tsacalculator;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.timeseries.arima.Arima;
import com.alibaba.alink.operator.common.timeseries.arima.ArimaModel;
import com.alibaba.alink.operator.common.timeseries.sarima.Sarima;
import com.alibaba.alink.operator.common.timeseries.sarima.SarimaModel;
import com.alibaba.alink.params.timeseries.ArimaParamsOld;
import com.alibaba.alink.params.timeseries.HasEstmateMethod;

import java.util.ArrayList;

public class ArimaPredictorCalc extends TimeSeriesPredictorCalc {

	private static final long serialVersionUID = -5965170186690618039L;
	HasEstmateMethod.EstMethod estMethod;
	Integer[] order;
	int[] seasonality;

	int ifIntercept;

	ArimaPredictorCalc() {}

	public ArimaPredictorCalc(Params params) {
		estMethod = params.get(ArimaParamsOld.EST_METHOD);
		predictNum = params.get(ArimaParamsOld.PREDICT_NUM);
		order = params.get(ArimaParamsOld.ORDER);
		seasonality = params.get(ArimaParamsOld.SEASONAL_ORDER);

		int d = order[1];
		ifIntercept = 1;
		if (d > 0) {
			ifIntercept = 0;
		}

		if (predictNum > 21) {
			throw new RuntimeException("Long step prediction is not meaningful. " +
				"The limitation is 20 steps. " +
				"Please set forecasteStep to be smaller than 21");
		}
	}

	@Override
	public double[] forecastWithoutException(double[] data, int forecastStep, boolean trainBeforeForecast) {

		ArimaModel model = Arima.fit(data, order[0], order[1], order[2], estMethod);

		return model.forecast(forecastStep).get(0);
	}

	@Override
	public double[] predict(double[] data) {
		return forecast(data, predictNum, true).f0;
	}

	@Override
	public Row map(Row in) {
		double[] data = (double[]) in.getField(groupNumber + 1);
		if (seasonality == null) {
			return arima(in, data);
		} else {
			return seasonalArima(in, data);
		}
	}

	@Override
	public ArimaPredictorCalc clone() {
		ArimaPredictorCalc calc = new ArimaPredictorCalc();
		calc.estMethod = estMethod;
		if (order != null) {
			calc.order = order.clone();
		}
		if (seasonality != null) {
			calc.seasonality = seasonality.clone();
		}
		calc.ifIntercept = ifIntercept;
		calc.predictNum = predictNum;
		calc.groupNumber = groupNumber;
		return calc;
	}

	private Row arima(Row in, double[] data) {
		ArimaModel model = Arima.fit(data, order[0], order[1], order[2], estMethod);

		ArrayList <double[]> forecast = model.forecast(this.predictNum);
		return getData(in,
			new DenseVector(forecast.get(0)),
			new DenseVector(forecast.get(1)),
			new DenseVector(forecast.get(2)),
			new DenseVector(forecast.get(3)),
			new DenseVector(model.arma.estimate.arCoef),
			new DenseVector(model.arma.estimate.arCoefStdError),
			new DenseVector(model.arma.estimate.maCoef),
			new DenseVector(model.arma.estimate.maCoefStdError),
			model.arma.estimate.intercept,
			model.arma.estimate.interceptStdError,
			model.arma.estimate.variance,
			model.arma.estimate.varianceStdError,
			model.ic,
			model.arma.estimate.logLikelihood);
	}

	private Row seasonalArima(Row in, double[] data) {

		SarimaModel as = Sarima.fit(data, order[0], order[1], order[2], seasonality[0], seasonality[1], seasonality[1],
			estMethod, ifIntercept, 2);

		ArrayList <double[]> forecast = as.forecast(this.predictNum);
		return getData(in,
			new DenseVector(forecast.get(0)),
			new DenseVector(forecast.get(1)),
			new DenseVector(forecast.get(2)),
			new DenseVector(forecast.get(3)),
			new DenseVector(as.sARCoef),
			new DenseVector(as.sArStdError),
			new DenseVector(as.sMACoef),
			new DenseVector(as.sMaStdError),
			new DenseVector(as.arima.arma.estimate.arCoef),
			new DenseVector(as.arima.arma.estimate.arCoefStdError),
			new DenseVector(as.arima.arma.estimate.maCoef),
			new DenseVector(as.arima.arma.estimate.maCoefStdError),
			as.arima.arma.estimate.intercept,
			as.arima.arma.estimate.interceptStdError,
			as.arima.arma.estimate.variance,
			as.arima.ic,
			as.arima.arma.estimate.logLikelihood);
	}
}
