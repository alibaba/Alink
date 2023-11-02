package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.timeseries.garch.Garch;
import com.alibaba.alink.operator.common.timeseries.garch.GarchModel;
import com.alibaba.alink.params.timeseries.AutoGarchParams;
import com.alibaba.alink.params.timeseries.HasIcType;

import java.sql.Timestamp;

public class AutoGarchMapper extends TimeSeriesSingleMapper {

	private HasIcType.IcType ic;
	private int upperbound;
	private boolean ifGARCH11;
	private boolean minusMean;
	private int predictNum;

	public AutoGarchMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		ic = params.get(AutoGarchParams.IC_TYPE);
		upperbound = params.get(AutoGarchParams.MAX_ORDER);
		predictNum = params.get(AutoGarchParams.PREDICT_NUM);
		ifGARCH11 = params.get(AutoGarchParams.IF_GARCH11);
		minusMean = params.get(AutoGarchParams.MINUS_MEAN);
	}

	@Override
	protected Tuple2 <double[], String> predictSingleVar(Timestamp[] historyTimes,
														 double[] historyVals,
														 int predictNum) {

		boolean ifGoodFit = true;
		GarchModel model = Garch.autoFit(historyVals, upperbound, minusMean, ic, ifGARCH11);

		GarchInfo garchInfo = new GarchInfo();
		double[] forecast = null;

		if (ifGoodFit) {
			forecast = model.forecast(this.predictNum);
			garchInfo.forecast = model.forecast(this.predictNum);
			garchInfo.alpha = model.alpha;
			garchInfo.alphaStdError = model.seAlpha;
			garchInfo.beta = model.beta;
			garchInfo.betaStdError = model.seBeta;
			garchInfo.c = model.c;
			garchInfo.cStdError = model.seC;
			garchInfo.unconSigma2 = model.unconSigma2;
			garchInfo.ic = model.ic;
			garchInfo.logLikelihood = model.loglike;
			garchInfo.hHat = model.hHat;
			garchInfo.residual = model.residual;
		}

		return Tuple2.of(forecast, JsonConverter.toJson(garchInfo));
	}

	private static class GarchInfo {
		double[] forecast;
		double[] alpha;
		double[] alphaStdError;
		double[] beta;
		double[] betaStdError;

		double c;
		double cStdError;
		double unconSigma2;
		double ic;
		double logLikelihood;

		double[] hHat;
		double[] residual;
	}
}