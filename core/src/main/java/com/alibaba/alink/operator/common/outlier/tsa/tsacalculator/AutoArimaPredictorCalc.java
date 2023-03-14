package com.alibaba.alink.operator.common.outlier.tsa.tsacalculator;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.timeseries.arima.Arima;
import com.alibaba.alink.operator.common.timeseries.arima.ArimaModel;
import com.alibaba.alink.operator.common.timeseries.sarima.Sarima;
import com.alibaba.alink.operator.common.timeseries.sarima.SarimaModel;
import com.alibaba.alink.params.outlier.tsa.baseparams.BaseStreamPredictParams;
import com.alibaba.alink.params.timeseries.AutoArimaParams;
import com.alibaba.alink.params.timeseries.HasEstmateMethod;
import com.alibaba.alink.params.timeseries.HasIcType;

import java.util.ArrayList;

public class AutoArimaPredictorCalc extends TimeSeriesPredictorCalc {
	private static final long serialVersionUID = 1472791946534368311L;
	private HasEstmateMethod.EstMethod estMethod;
	private HasIcType.IcType ic;
	private int maxOrder;
	private int seasonalPeriod;
	private int maxSeasonalOrder;

	AutoArimaPredictorCalc() {}

	public AutoArimaPredictorCalc(Params params) {
		estMethod = params.get(AutoArimaParams.EST_METHOD);
		ic = params.get(AutoArimaParams.IC_TYPE);
		predictNum = params.get(BaseStreamPredictParams.PREDICT_NUM);
		maxOrder = params.get(AutoArimaParams.MAX_ORDER);
		seasonalPeriod = params.get(AutoArimaParams.MAX_SEASONAL_ORDER);

		if (predictNum > 21) {
			throw new RuntimeException("Long step prediction is not meaningful. " +
				"The limitation is 20 steps. " +
				"Please set forecasteStep to be smaller than 21");
		}
	}

	private Row arima(Row in, double[] data) {
		ArimaModel model = Arima.autoFit(data, maxOrder, estMethod, ic,-1);

		if (model.isGoodFit()) {
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
		} else {
			return getData(in,
				null, null, null, null,
				null, null, null, null,
				null, null, null, null,
				null, null);
		}

	}

	private Row seasonalArima(Row key, double[] data) {
		SarimaModel bestModel = Sarima.autoFit(
			data, maxOrder, maxSeasonalOrder,
			estMethod, ic, seasonalPeriod);

		Row newRow = new Row(18);
		if (bestModel.isGoodFit()) {
			newRow.setField(0, key);
			ArrayList <double[]> forecast = bestModel.forecast(this.predictNum);
			return getData(key,
				new DenseVector(forecast.get(0)),
				new DenseVector(forecast.get(1)),
				new DenseVector(forecast.get(2)),
				new DenseVector(forecast.get(3)),
				new DenseVector(bestModel.sARCoef),
				new DenseVector(bestModel.sArStdError),
				new DenseVector(bestModel.sMACoef),
				new DenseVector(bestModel.sMaStdError),
				new DenseVector(bestModel.arima.arma.estimate.arCoef),
				new DenseVector(bestModel.arima.arma.estimate.arCoefStdError),
				new DenseVector(bestModel.arima.arma.estimate.maCoef),
				new DenseVector(bestModel.arima.arma.estimate.maCoefStdError),
				bestModel.arima.arma.estimate.intercept,
				bestModel.arima.arma.estimate.variance,
				bestModel.ic,
				bestModel.arima.arma.estimate.logLikelihood);
		} else {
			newRow.setField(0, key);
			for (int i = 1; i < newRow.getArity(); i++) {
				newRow.setField(i, null);
			}
		}
		return newRow;
	}

	@Override
	public double[] forecastWithoutException(double[] data, int forecastStep, boolean trainBeforeForecast) {
		if (seasonalPeriod == 1) {
			ArimaModel model = Arima.autoFit(data, maxOrder, estMethod, ic, -1);
			if (model.isGoodFit()) {
				return model.forecast(forecastStep).get(0);
			} else {
				return null;
			}
		} else {
			SarimaModel bestModel = Sarima.autoFit(data, maxOrder,
				maxSeasonalOrder, estMethod, ic,
				seasonalPeriod);
			if (bestModel.isGoodFit()) {
				return bestModel.forecast(forecastStep).get(0);
			} else {
				return null;
			}
		}
	}

	@Override
	public double[] predict(double[] data) {
		return forecast(data, this.predictNum, true).f0;
	}

	@Override
	public Row map(Row in) {

		double[] data = (double[]) in.getField(groupNumber + 1);
		if (seasonalPeriod == 1) {
			return arima(in, data);
		} else {
			return seasonalArima(in, data);
		}
	}

	@Override
	public AutoArimaPredictorCalc clone() {
		AutoArimaPredictorCalc calc = new AutoArimaPredictorCalc();
		calc.predictNum = predictNum;
		calc.groupNumber = groupNumber;
		calc.estMethod = estMethod;
		calc.ic = ic;
		calc.maxOrder = maxOrder;
		calc.seasonalPeriod = seasonalPeriod;
		return calc;
	}
}
