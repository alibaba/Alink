package com.alibaba.alink.operator.common.outlier.tsa.tsacalculator;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.outlier.TimeSeriesAnomsUtils;
import com.alibaba.alink.params.timeseries.HasFrequency;
import com.alibaba.alink.params.timeseries.holtwinters.HasSeasonalType;

import java.util.Arrays;

public class ConvolutionDecomposerCalc extends TimeSeriesDecomposerCalc {

	private boolean isAddType;
	private int frequency;

	public ConvolutionDecomposerCalc(Params params) {
		frequency = params.get(HasFrequency.FREQUENCY);
		isAddType = params.get(HasSeasonalType.SEASONAL_TYPE) == HasSeasonalType.SeasonalType.ADDITIVE;
	}

	//used in holt winters decompose.
	@Override
	public DenseVector[] decompose(double[] data) {
		boolean addType = this.isAddType;
		double[] filter;
		double filterData = 1.0 / frequency;
		if (frequency % 2 == 0) {
			filter = new double[frequency + 1];
			Arrays.fill(filter, filterData);
			filter[0] /= 2;
			filter[frequency] /= 2;
		} else {
			filter = new double[frequency];
			Arrays.fill(filter, filterData);
		}
		//the length of filter equals to frequency.
		//trend is the convolution of data.
		double[] trend = cFilter(data, filter);
		double[] seasonal = data.clone();
		if (addType) {
			for (int i = 0; i < seasonal.length; i++) {
				seasonal[i] -= trend[i];
			}
		} else {
			for (int i = 0; i < seasonal.length; i++) {
				seasonal[i] /= trend[i];
			}
		}

		int period = data.length / frequency;
		double[] figure = new double[frequency];
		double tmp1;
		int num1;
		for (int i = 0; i < figure.length; i++) {
			tmp1 = 0;
			num1 = 0;
			for (int j = 0; j < period; j++) {
				if (Double.isNaN(seasonal[i + j * frequency])) {
					continue;
				}
				tmp1 += seasonal[i + j * frequency];
				num1 += 1;
			}
			figure[i] = tmp1 / num1;
		}

		double mean = TimeSeriesAnomsUtils.mean(figure);
		double[] reminder = new double[data.length];
		if (addType) {
			for (int i = 0; i < figure.length; i++) {
				figure[i] -= mean;
			}
			for (int i = 0; i < data.length; i++) {
				seasonal[i] = figure[i % frequency];
				reminder[i] = data[i] - trend[i] - seasonal[i];
			}
		} else {
			for (int i = 0; i < figure.length; i++) {
				figure[i] /= mean;
			}
			for (int i = 0; i < data.length; i++) {
				seasonal[i] = figure[i % frequency];
				reminder[i] = data[i] / trend[i] / seasonal[i];
			}
		}
		return new DenseVector[] {new DenseVector(trend), new DenseVector(seasonal), new DenseVector(reminder)};
	}

	//used in holt winters decompose.
	private static double[] cFilter(double[] data, double[] filter) {
		int start = 0;
		int end = data.length;
		int allSize = end - start;
		double[] filterData = new double[allSize];
		Arrays.fill(filterData, Double.NaN);
		int nf = filter.length;
		int nShift = nf / 2;
		double z;
		boolean flag = true;
		for (int i = (start + nShift); i < (end - nShift); i++) {
			z = 0;
			//卷积操作
			for (int j = (i - nShift); j < (i + nShift + 1); j++) {
				if (Double.isNaN(data[j])) {
					flag = false;
					break;
				}
				z += data[j] * filter[j + nShift - i];
			}
			if (flag) {
				filterData[i] = z;
			}
			flag = true;
		}
		return filterData;
	}

}
