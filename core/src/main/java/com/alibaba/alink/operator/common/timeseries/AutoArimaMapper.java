package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.operator.common.timeseries.sarima.Sarima;
import com.alibaba.alink.operator.common.timeseries.sarima.SarimaModel;
import com.alibaba.alink.params.timeseries.AutoArimaParams;
import com.alibaba.alink.params.timeseries.HasEstmateMethod;
import com.alibaba.alink.params.timeseries.HasIcType;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;

public class AutoArimaMapper extends TimeSeriesSingleMapper {

	private HasEstmateMethod.EstMethod estMethod;
	private HasIcType.IcType ic;
	private int maxOrder;
	private int maxSeasonalOrder;
	private int predictNum;
	private int seasonalPeriod;
	private int d;

	public AutoArimaMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		estMethod = params.get(AutoArimaParams.EST_METHOD);
		ic = params.get(AutoArimaParams.IC_TYPE);
		predictNum = params.get(AutoArimaParams.PREDICT_NUM);
		maxOrder = params.get(AutoArimaParams.MAX_ORDER);
		maxSeasonalOrder = params.get(AutoArimaParams.MAX_SEASONAL_ORDER);
		seasonalPeriod = params.get(AutoArimaParams.SEASONAL_PERIOD);
		d = params.get(AutoArimaParams.D);
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

		if (isEqualSeries || historyVals.length <= 2) {
			double[] predValues = new double[predictNum];
			Arrays.fill(predValues, historyVals[historyVals.length - 1]);
			return Tuple2.of(predValues, null);
		}

		SarimaModel bestModel = Sarima.autoFit(historyVals,
			maxOrder,
			maxSeasonalOrder,
			estMethod,
			ic,
			seasonalPeriod,
			d);

		if (bestModel.isGoodFit()) {
			ArrayList <double[]> forecast = bestModel.forecast(this.predictNum);
			return ArimaMapper.toArimaInfo(bestModel, forecast);
		} else {
			return Tuple2.of(null, "It is not good fit.");
		}
	}

}
