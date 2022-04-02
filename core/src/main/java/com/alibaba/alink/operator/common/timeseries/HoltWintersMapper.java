package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.operator.common.timeseries.holtwinter.HoltWinters;
import com.alibaba.alink.operator.common.timeseries.holtwinter.HoltWintersModel;
import com.alibaba.alink.params.timeseries.HoltWintersParams;
import com.alibaba.alink.params.timeseries.holtwinters.HasLevelStart;
import com.alibaba.alink.params.timeseries.holtwinters.HasSeasonalStart;
import com.alibaba.alink.params.timeseries.holtwinters.HasSeasonalType;
import com.alibaba.alink.params.timeseries.holtwinters.HasSeasonalType.SeasonalType;
import com.alibaba.alink.params.timeseries.holtwinters.HasTrendStart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Arrays;

public class HoltWintersMapper extends TimeSeriesSingleMapper {

	private static final long serialVersionUID = 6653124016287841989L;
	private static final Logger LOG = LoggerFactory.getLogger(HoltWintersMapper.class);

	private double alpha;
	private double beta;
	private double gamma;
	private int frequency;
	//levelStart, trendStart, seasonalStart is the initial data of level, trend and seasonalPeriod.
	private Double levelStart;
	private Double trendStart;
	private double[] seasonalStart;
	private boolean doTrend;
	private boolean doSeasonal;
	private SeasonalType seasonalType;

	public HoltWintersMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		frequency = params.get(HoltWintersParams.FREQUENCY);
		alpha = params.get(HoltWintersParams.ALPHA);
		beta = params.get(HoltWintersParams.BETA);
		gamma = params.get(HoltWintersParams.GAMMA);
		doTrend = params.get(HoltWintersParams.DO_TREND);
		doSeasonal = params.get(HoltWintersParams.DO_SEASONAL);

		if (doSeasonal && !doTrend) {
			throw new RuntimeException("seasonal time serial must have trend.");
		}
		seasonalType = params.get(HasSeasonalType.SEASONAL_TYPE);

		if (params.contains(HasLevelStart.LEVEL_START)) {
			levelStart = params.get(HasLevelStart.LEVEL_START);
		}
		if (params.contains(HasTrendStart.TREND_START)) {
			trendStart = params.get(HasTrendStart.TREND_START);
		}
		if (params.contains(HasSeasonalStart.SEASONAL_START)) {
			seasonalStart = params.get(HasSeasonalStart.SEASONAL_START);
			if (seasonalStart.length != frequency) {
				throw new RuntimeException("the length of " +
					"seasonal start data must equal to frequency.");
			}
		}
	}

	@Override
	protected Tuple2 <double[], String> predictSingleVar(Timestamp[] historyTimes,
														 double[] historyVals,
														 int predictNum) {
		try {
			if (doTrend) {
				if (2 * frequency > historyVals.length) {
					return Tuple2.of(null, null);
				}
			}

			HoltWintersModel model = adapterFit(historyVals.clone());

			return model == null ? Tuple2.of(null, null) : Tuple2.of(model.forecast(predictNum), null);

		} catch (Exception ex) {
			ex.printStackTrace();
			return Tuple2.of(null, null);
		}
	}

	private HoltWintersModel adapterFit(double[] historyVals) {
		while (true) {
			try {
				return HoltWinters.fit(historyVals, frequency,
					alpha, beta, gamma, doTrend, doSeasonal,
					seasonalType, levelStart, trendStart, seasonalStart);
			} catch (Throwable ex) {
				LOG.info(ex.toString());
				LOG.info("val length -1 ");
				double[] newVals = Arrays.copyOfRange(historyVals, 1, historyVals.length);
				if (2 >= newVals.length) {
					System.out.println("holtwinter error." + new DoubleTensor(historyVals).toDisplayData());
					return null;
				}
				if (doTrend && (2 * frequency > newVals.length)) {
					System.out.println("holtwinter error." + new DoubleTensor(historyVals).toDisplayData());
					return null;
				}
				historyVals = newVals;
			}
		}
	}
}