package com.alibaba.alink.operator.common.timeseries.sarima;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.operator.common.timeseries.TsMethod;
import com.alibaba.alink.operator.common.timeseries.arima.Arima;
import com.alibaba.alink.operator.common.timeseries.arima.ArimaModel;
import com.alibaba.alink.operator.common.timeseries.teststatistics.KPSS;
import com.alibaba.alink.operator.common.timeseries.teststatistics.StationaryTest;
import com.alibaba.alink.params.timeseries.HasEstmateMethod.EstMethod;
import com.alibaba.alink.params.timeseries.HasIcType.IcType;

public class Sarima {

	/*
	 * SARIMAModel estimate multiple seasonal ARIMA model separately.
	 * Firstly find residuals of seasonal ARMA modes after seasonal differencing. Then use the residual to fit
	 * ARIMA
	 *
	 * order=[p, d, q]
	 * seasonal order: sOrder=[P, D, Q]
	 * estMethod: Estimating method for both seasonal part and non seasonal part. "Mom" is not supported in SARIMA
	 * ifIntercept: If the model should contain intercept in nonseasonal part. Assumed there is no intercept for
	 * seasonal part
	 * seasonalPeriod: Period of seasonality. If seasonalPeriod==0, SARIMA becomes simple ARIMA
	 */
	public static SarimaModel fit(double[] data,
								  int p, int d, int q,
								  int sP, int sD, int sQ,
								  EstMethod estMethod,
								  int ifIntercept,
								  int seasonalPeriod) {

		SarimaModel model = new SarimaModel(p, d, q, sP, sD, sQ, estMethod, ifIntercept, seasonalPeriod);
		if (seasonalPeriod < 1) {
			throw new AkIllegalOperatorParameterException(
				"Seasonality must be equal or Greater than 1. If it is 1, there is no seasonality.");
		}
		if (p < 0 || q < 0 || d < 0) {
			throw new AkIllegalOperatorParameterException("Order p, d and q must equal to or Greater than 0");
		}
		if (seasonalPeriod > 1 && (sP < 0 || sQ < 0 || sD < 0)) {
			throw new AkIllegalOperatorParameterException("Seasonal order p, d and q must >=0 0 when seasonalPeriod > 2");
		}
		if (data == null) {
			throw new AkIllegalDataException("Data is null.");
		}
		if (seasonalPeriod > 1 && (data.length - sD * seasonalPeriod - d - p < p + q + sP + sQ + ifIntercept)) {
			throw new AkIllegalDataException(
				"Do not have enough data. Please reduce order and seasonal order, or add sample.");
		}

		if (seasonalPeriod == 1) {
			model.arima = Arima.fit(data, p, d, q, estMethod);
		} else {
			model.seasonDData = new double[sD + 1][data.length];
			model.seasonDData[0] = data.clone();

			//D part
			for (int i = 1; i <= sD; i++) {
				model.seasonDData[i] = TsMethod.seasonDiff(i, seasonalPeriod, model.seasonDData[i - 1]);
			}

			double[] seasonDDataLast = new double[data.length - sD * seasonalPeriod];
			System.arraycopy(model.seasonDData[sD], sD * seasonalPeriod,
				seasonDDataLast, 0, seasonDDataLast.length);

			model.seasonMatrix = TsMethod.seasonArray2Matrix(seasonDDataLast, seasonalPeriod);

			double[] sResidual = null;
			if (sP == 0 && sQ == 0) {
				model.sResidual = seasonDDataLast.clone();
				sResidual = seasonDDataLast.clone();
			} else {
				int initPoint = seasonalPeriod * sP;
				SarimaEstimate estimate = null;
				switch (estMethod) {
					case Css:
						estimate = new SCSSEstimate();
						break;
					case CssMle:
						estimate = new SCSSMLEEstimate();
						break;
					default:
						throw new AkUnsupportedOperationException(String.format("Estimation method [%s] not support.", estMethod));

				}

				estimate.compute(model.seasonMatrix, sP, sQ, seasonalPeriod);

				model.sARCoef = estimate.sARCoef;
				model.sMACoef = estimate.sMACoef;
				model.sArStdError = estimate.sArStdError;
				model.sMaStdError = estimate.sMaStdError;
				model.sResidual = estimate.sResidual;

				sResidual = new double[data.length - initPoint];
				System.arraycopy(model.sResidual, initPoint, sResidual, 0,
					model.sResidual.length - initPoint);
			}

			model.arima = Arima.fit(sResidual, p, d, q, estMethod);

			model.sampleSize = sResidual.length - p;
		}
		return model;
	}

	public static SarimaModel autoFit(double[] data,
									  int maxOrder,
									  int maxSeasonalOrder,
									  EstMethod estMethod,
									  IcType ic,
									  int seasonalPeriod) {
		return autoFit(data, maxOrder, maxSeasonalOrder, estMethod, ic, seasonalPeriod, -1);
	}

	public static SarimaModel autoFit(double[] data,
									  int maxOrder,
									  int maxSeasonalOrder,
									  EstMethod estMethod,
									  IcType ic,
									  int seasonalPeriod,
									  int d) {

		if (seasonalPeriod == 1) {
			ArimaModel model = Arima.autoFit(data, maxOrder, estMethod, ic, d);
			int ifIntercept = model.d > 0 ? 0 : 1;
			SarimaModel sModel = new SarimaModel(model.p, model.d, model.q,
				-1, -1, -1, estMethod, ifIntercept, 1);
			sModel.arima = model;
			return sModel;
		}

		//step 1: create seasonal differenced data and origin data
		int sD1 = 0;
		int sD2 = 1;
		double[] kpssData = data.clone();

		//step 2: find d
		StationaryTest st = new StationaryTest();

		int diff1 = 0;
		for (int i = 0; i < 6; i++) {
			KPSS kpss = st.kpss(kpssData);
			if (kpss.cnValue < kpss.cnCritic) {
				break;
			} else {
				diff1 += 1;
				kpssData = st.differenceFromZero(kpssData);
			}
		}

		if (diff1 == 6) {
			throw new AkIllegalDataException("1-lag difference can not change data to stationary series.");
		}

		int d1 = diff1;

		//step 3: find best arimas via IC
		SarimaModel sarima1 = TsMethod.seasonalStepWiseOrderSelect(
			data, maxOrder, maxSeasonalOrder, estMethod, ic, d1,
			sD1, seasonalPeriod, true);

		if (!sarima1.isGoodFit()) {
			d1 += 1;
			sarima1 = TsMethod.seasonalStepWiseOrderSelect(
				data, maxOrder, maxSeasonalOrder, estMethod, ic, d1, sD1,
				seasonalPeriod, true);
		}

		if (!sarima1.isGoodFit()) {
			d1 += 1;
			sarima1 = TsMethod.seasonalStepWiseOrderSelect(
				data, maxOrder, maxSeasonalOrder, estMethod, ic, d1, sD1,
				seasonalPeriod, true);
		}

		int diff2 = 0;
		double[] sdData = TsMethod.seasonDiff(1, seasonalPeriod, data);

		for (int i = 0; i < 6; i++) {
			KPSS kpss = st.kpss(sdData);
			if (kpss.cnValue < kpss.cnCritic) {
				break;
			} else {
				diff2 += 1;
				sdData = st.differenceFromZero(sdData);
			}
		}

		if (diff2 == 6) {
			throw new AkIllegalDataException("1-lag difference can not change data to stationary series.");
		}

		int d2 = diff2;

		SarimaModel sarima2 = TsMethod.seasonalStepWiseOrderSelect(
			sdData, maxOrder, maxSeasonalOrder, estMethod, ic, d2, sD2,
			seasonalPeriod, true);

		if (!sarima2.isGoodFit()) {
			d2 += 1;
			sarima2 = TsMethod.seasonalStepWiseOrderSelect(
				sdData, maxOrder, maxSeasonalOrder, estMethod, ic, d2, sD2,
				seasonalPeriod, true);
		}

		if (!sarima2.isGoodFit()) {
			d2 += 1;
			sarima2 = TsMethod.seasonalStepWiseOrderSelect(
				sdData, maxOrder, maxSeasonalOrder, estMethod, ic, d2, sD2,
				seasonalPeriod, true);
		}

		if (sarima1.ic <= sarima2.ic) {
			sarima1 = sarima2;
		}

		return sarima1;

	}

}
