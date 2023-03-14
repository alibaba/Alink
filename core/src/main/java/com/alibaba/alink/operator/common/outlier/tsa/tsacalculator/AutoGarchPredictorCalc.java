package com.alibaba.alink.operator.common.outlier.tsa.tsacalculator;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.timeseries.garch.Garch;
import com.alibaba.alink.operator.common.timeseries.garch.GarchModel;
import com.alibaba.alink.params.outlier.tsa.baseparams.BaseStreamPredictParams;
import com.alibaba.alink.params.timeseries.AutoGarchParams;
import com.alibaba.alink.params.timeseries.HasIcType;

public class AutoGarchPredictorCalc extends TimeSeriesPredictorCalc {
	private static final long serialVersionUID = -446589576860978836L;
	private HasIcType.IcType ic;
	private int upperbound;
	private boolean ifGARCH11;
	private boolean minusMean;

	AutoGarchPredictorCalc() {}

	public AutoGarchPredictorCalc(Params params) {
		ic = params.get(AutoGarchParams.IC_TYPE);
		upperbound = params.get(AutoGarchParams.MAX_ORDER);
		predictNum = params.get(BaseStreamPredictParams.PREDICT_NUM);
		ifGARCH11 = params.get(AutoGarchParams.IF_GARCH11);
		minusMean = params.get(AutoGarchParams.MINUS_MEAN);
	}

	@Override
	public double[] forecastWithoutException(double[] data, int forecastStep, boolean trainBeforeForecast) {
		GarchModel ag = Garch.autoFit(data, upperbound, minusMean, ic, ifGARCH11);
		if (!ag.isGoodFit()) {
			throw new RuntimeException("fail to fit the Garch model.");
		}
		return ag.forecast(forecastStep);
	}

	@Override
	public double[] predict(double[] data) {
		GarchModel ag = Garch.autoFit(data, upperbound, minusMean, ic, ifGARCH11);
		if (ag.isGoodFit()) {
			return ag.forecast(this.predictNum);
		}
		return null;
	}

	@Override
	public Row map(Row in) {
		double[] data = (double[]) in.getField(groupNumber + 1);
		GarchModel ag = Garch.autoFit(data, upperbound, minusMean, ic, ifGARCH11);

		if (ag.isGoodFit()) {
			double[] forecast = ag.forecast(this.predictNum);
			return getData(in,
				new DenseVector(forecast),
				new DenseVector(ag.alpha),
				new DenseVector(ag.seAlpha),
				new DenseVector(ag.beta),
				new DenseVector(ag.seBeta),
				ag.c,
				ag.seC,
				ag.unconSigma2,
				ag.ic,
				ag.loglike,
				new DenseVector(ag.hHat),
				new DenseVector(ag.residual));

		} else {
			return getData(in, null, null, null, null,
				null, null, null, null,
				null, null, null, null);
		}
	}

	@Override
	public AutoGarchPredictorCalc clone() {
		AutoGarchPredictorCalc calc = new AutoGarchPredictorCalc();
		calc.predictNum = predictNum;
		calc.groupNumber = groupNumber;
		calc.ic = ic;
		calc.upperbound = upperbound;
		calc.ifGARCH11 = ifGARCH11;
		calc.minusMean = minusMean;
		return calc;
	}
}
