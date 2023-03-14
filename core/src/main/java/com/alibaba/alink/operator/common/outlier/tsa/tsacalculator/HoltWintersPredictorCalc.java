package com.alibaba.alink.operator.common.outlier.tsa.tsacalculator;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.timeseries.holtwinter.HoltWinters;
import com.alibaba.alink.operator.common.timeseries.holtwinter.HoltWintersModel;
import com.alibaba.alink.params.timeseries.HasFrequency;
import com.alibaba.alink.params.outlier.tsa.HasPredictNum;
import com.alibaba.alink.params.timeseries.holtwinters.HasAlpha;
import com.alibaba.alink.params.timeseries.holtwinters.HasBeta;
import com.alibaba.alink.params.timeseries.holtwinters.HasDoSeasonal;
import com.alibaba.alink.params.timeseries.holtwinters.HasDoTrend;
import com.alibaba.alink.params.timeseries.holtwinters.HasGamma;
import com.alibaba.alink.params.timeseries.holtwinters.HasLevelStart;
import com.alibaba.alink.params.timeseries.holtwinters.HasSeasonalStart;
import com.alibaba.alink.params.timeseries.holtwinters.HasSeasonalType;
import com.alibaba.alink.params.timeseries.holtwinters.HasSeasonalType.SeasonalType;
import com.alibaba.alink.params.timeseries.holtwinters.HasTrendStart;

public class HoltWintersPredictorCalc extends TimeSeriesPredictorCalc {
	private static final long serialVersionUID = 6298595998408725962L;
	private double alpha;
	private double beta;
	private double gamma;
	private int size;
	private boolean isAddType;
	private SeasonalType seasonalType;
	boolean doTrend;
	boolean doSeasonal;
	private int frequency;
	//a, b, s is the initial data of level, trend and seasonalPeriod.
	private Double a;
	private Double b;
	private double[] s;
	private DenseVector res;
	private Double sse;
	//for reset
	private Double saveA;
	private Double saveB;
	private double[] saveS;
	private DenseVector saveRes;

	HoltWintersPredictorCalc() {}

	public HoltWintersPredictorCalc(Params params) {
		initParams(params);
		saveA = a;
		saveB = b;
		saveS = s;
		saveRes = res;
	}

	@Override
	public void reset() {
		a = saveA;
		b = saveB;
		s = saveS;
		res = saveRes;
	}

	@Override
	public HoltWintersPredictorCalc clone() {
		HoltWintersPredictorCalc calc = new HoltWintersPredictorCalc();
		calc.alpha = alpha;
		calc.beta = beta;
		calc.gamma = gamma;
		calc.size = size;
		calc.isAddType = isAddType;
		calc.frequency = frequency;
		calc.a = a;
		calc.b = b;
		if (s != null) {
			calc.s = s.clone();
		}
		if (res != null) {
			calc.res = res.clone();
		}
		calc.saveA = saveA;
		calc.saveB = saveB;
		if (saveS != null) {
			calc.saveS = saveS.clone();
		}
		if (saveRes != null) {
			calc.saveRes = saveRes.clone();
		}
		return calc;
	}

	@Override
	public double[] forecastWithoutException(double[] data, int forecastStep, boolean overWritten) {
		HoltWintersModel model = HoltWinters.fit(data,  frequency,
		 alpha,
		 beta,
		 gamma,
		 doTrend,
		 doSeasonal,
		 seasonalType,
		 a,
		 b,
		 s);

		return model.forecast(predictNum);
	}

	@Override
	public double[] predict(double[] data) {
		if (predictNum == null) {
			throw new RuntimeException("Please set forecast number first!");
		}
		return forecast(data, predictNum, false).f0;
	}

	@Override
	public Row map(Row in) {
		double[] data = (double[]) in.getField(groupNumber + 1);
		data = predict(data);
		return getData(in, new DenseVector(data));
	}


	private void initParams(Params params) {
		frequency = params.get(HasFrequency.FREQUENCY);
		alpha = params.get(HasAlpha.ALPHA);
		beta = params.get(HasBeta.BETA);
		gamma = params.get(HasGamma.GAMMA);
		predictNum = params.get(HasPredictNum.PREDICT_NUM);
		doTrend = params.get(HasDoTrend.DO_TREND);
		doSeasonal = params.get(HasDoSeasonal.DO_SEASONAL);
		//level是一定会有的。而且alpha, beta, gamma有初始值。所以只需要判断这一个就好了。
		if (doSeasonal && !doTrend) {
			throw new RuntimeException("seasonal time serial must have trend.");
		}
		isAddType = params.get(HasSeasonalType.SEASONAL_TYPE) == HasSeasonalType.SeasonalType.ADDITIVE;
		if (doSeasonal) {
			size = 3;
		} else if (doTrend) {
			size = 2;
		} else {
			size = 1;
		}

		if (params.contains(HasLevelStart.LEVEL_START)) {
			a = params.get(HasLevelStart.LEVEL_START);
		}
		if (params.contains(HasTrendStart.TREND_START)) {
			b = params.get(HasTrendStart.TREND_START);
		}
		if (params.contains(HasSeasonalStart.SEASONAL_START)) {
			s = params.get(HasSeasonalStart.SEASONAL_START);
			if (s.length != frequency) {
				throw new RuntimeException("the length of " +
					"seasonal start data must equal to frequency.");
			}
		}
		seasonalType = params.get(HasSeasonalType.SEASONAL_TYPE);
	}

}
