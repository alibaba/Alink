package com.alibaba.alink.operator.common.timeseries.arima;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.operator.common.timeseries.TsMethod;
import com.alibaba.alink.operator.common.timeseries.teststatistics.KPSS;
import com.alibaba.alink.operator.common.timeseries.teststatistics.StationaryTest;
import com.alibaba.alink.params.timeseries.HasEstmateMethod.EstMethod;
import com.alibaba.alink.params.timeseries.HasIcType.IcType;

/**
 * Arima: Autoregressive Integrated Moving Average model.
 */
public class Arima {

	/**
	 *
	 * @param data
	 * @param p
	 * @param d
	 * @param q
	 * @param estMethod
	 * @return ArimaModel
	 */
	public static ArimaModel fit(double[] data, int p, int d, int q, EstMethod estMethod) {
		int ifIntercept = d > 0 ? 0 : 1;
		return fit(data, p, d, q, estMethod, ifIntercept);
	}

	/*
	 * order=(p,d,q)
	 * estMethod:
	 * 		use MOM:1 and MLE:2 to estimate parameters.
	 * 		for MOM, Levinson algorithm is used to avoid unsolvable matrix.
	 *
	 */
	public static ArimaModel fit(double[] data, int p, int d, int q, EstMethod estMethod, int ifIntercept) {
		if (data == null) {
			throw new AkIllegalDataException("data must be not null.");
		}

		if (data.length - p - d < p + q + ifIntercept) {
			throw new AkIllegalDataException("Do not have enough data, data size must > 2p + d+ q.");
		}

		ArimaModel model = new ArimaModel(p, d, q, estMethod, ifIntercept);

		model.diffData = new double[d + 1][data.length];
		model.diffData[0] = data.clone();
		for (int i = 1; i <= d; i++) {
			model.diffData[i] = TsMethod.diff(i, model.diffData[i - 1]);
		}

		double[] dData = new double[data.length - d];
		System.arraycopy(model.diffData[d], d, dData, 0, dData.length);

		model.arma.fit(dData);

		return model;
	}

	/**
	 *
	 * @param data
	 * @return ArimaModel
	 */
	public static ArimaModel autoFit(double[] data) {
		return autoFit(data, 10, EstMethod.CssMle, IcType.AIC, -1);
	}

	/**
	 *
	 * @param data
	 * @param upperBound
	 * @param estMethod
	 * @param ic
	 * @param d
	 * @return
	 */
	public static ArimaModel autoFit(double[] data,
									 int upperBound,
									 EstMethod estMethod,
									 IcType ic, int d) {
		int diff = 0;

		//step 1: find d
		double[] data1 = data.clone();
		StationaryTest st = new StationaryTest();
		for (int i = 0; i < 6; i++) {
			KPSS kpss = st.kpss(data1);
			if (kpss.cnValue < kpss.cnCritic) {
				break;
			} else {
				diff += 1;
				data1 = st.differenceFromZero(data1);
			}
		}

		if (diff == 6) {
			throw new AkIllegalDataException("1-lag difference can not change data to stationary series.");
		}

		if (d >= 0 && diff != d) {
			return new ArimaModel();
		}

		//step 2: find best arima via IC
		ArimaModel arima = TsMethod.stepWiseOrderSelect(data,
			upperBound, estMethod, ic, diff, true);

		if (!arima.isGoodFit()) {
			diff += 1;
			arima = TsMethod.stepWiseOrderSelect(data,
				upperBound,
				estMethod,
				ic,
				diff,
				true);
		}

		if (!arima.isGoodFit()) {
			diff += 1;
			arima = TsMethod.stepWiseOrderSelect(data,
				upperBound,
				estMethod,
				ic,
				diff,
				true);
		}

		return arima;
	}

}
