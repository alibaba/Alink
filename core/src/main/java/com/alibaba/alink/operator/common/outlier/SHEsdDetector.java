package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.outlier.tsa.tsacalculator.STLDecomposerCalc;
import com.alibaba.alink.params.outlier.HasDirection;

import java.util.HashSet;
import java.util.Map;

public class SHEsdDetector extends OutlierDetector {

	private final int inputMaxIter;
	private double alpha;
	private HasDirection.Direction direction;
	private int frequency;
	private final String selectedCol;

	public SHEsdDetector(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		inputMaxIter = params.contains(SHEsdDetectorParams.MAX_ITER) ? params.get(SHEsdDetectorParams.MAX_ITER) : -1;
		alpha = params.get(SHEsdDetectorParams.SHESD_ALPHA);
		direction = params.get(SHEsdDetectorParams.DIRECTION);
		frequency = params.get(SHEsdDetectorParams.FREQUENCY);
		this.selectedCol = params.contains(SHEsdDetectorParams.SELECTED_COL) ? params.get(
			SHEsdDetectorParams.SELECTED_COL)
			: dataSchema.getFieldNames()[0];
	}

	@Override
	public Tuple3 <Boolean, Double, Map <String, String>>[] detect(MTable series, boolean detectLast) throws Exception {
		double[] data = OutlierUtil.getNumericArray(series, selectedCol);
		int dataNum = data.length;
		int maxIter = Math.min(this.inputMaxIter, dataNum / 2);
		if (maxIter < 0) {
			maxIter = (dataNum + 9) / 10;
		}
		Tuple3 <Boolean, Double, Map <String, String>>[] results = new Tuple3[dataNum];
		for (int i = 0; i < dataNum; i++) {
			results[i] = Tuple3.of(false, 0.1, null);
		}

		DenseVector[] components = STLDecomposerCalc.decompose(data, frequency);
		double[] trend = components[0].getData();
		double[] seasonal = components[1].getData();
		double dataMedian = CalcMidian.tempMedian(data);
		double[] dataDecomp = new double[dataNum];
		//here minus median and seasonal of data.
		for (int i = 0; i < dataNum; i++) {
			data[i] -= (dataMedian + seasonal[i]);
			dataDecomp[i] = trend[i] + seasonal[i];
		}
		CalcMidian calcMidian = new CalcMidian(data);
		int[] outlierIndex = new int[maxIter];
		double[] ares = new double[dataNum];
		HashSet <Integer> excludedIndices = new HashSet <>();
		for (int i = 1; i < maxIter + 1; i++) {
			double median = calcMidian.median();
			//area is the deviation value of each data.
			switch (direction) {
				case POSITIVE:
					for (int j = 0; j < dataNum; j++) {
						ares[j] = data[j] - median;
					}
					break;
				case NEGATIVE:
					for (int j = 0; j < dataNum; j++) {
						ares[j] = median - data[j];
					}
					break;
				case BOTH:
					for (int j = 0; j < dataNum; j++) {
						ares[j] = Math.abs(data[j] - median);
					}
					break;
				default:
			}
			double dataSigma = TimeSeriesAnomsUtils.mad(calcMidian);

			if (Math.abs(dataSigma) < 1e-4) {
				break;
			}
			double maxValue = -Double.MAX_VALUE;
			int maxIndex = -1;
			for (int j = 0; j < dataNum; j++) {
				if (!excludedIndices.contains(j)) {
					if (ares[j] > maxValue) {
						maxValue = ares[j];
						maxIndex = j;
					}
				}
			}
			//添加的时候就意味着计算中位数的地方要扣除相应的。
			calcMidian.remove(data[maxIndex]);
			excludedIndices.add(maxIndex);

			maxValue /= dataSigma;
			outlierIndex[i - 1] = maxIndex;
			double p;
			//tempNum is the sample num?
			int tempNum = dataNum - i + 1;
			if (direction == HasDirection.Direction.BOTH) {
				p = 1 - alpha / (2 * tempNum);
			} else {
				p = 1 - alpha / tempNum;
			}
			double t = TimeSeriesAnomsUtils.tppf(p, tempNum - 2);
			//lam is the hypothesis test condition.
			double lam = t * (tempNum - 1) / Math.sqrt((tempNum - 2 + t * t) * tempNum);
			if (maxValue > lam) {
				results[maxIndex].f0 = true;
				results[maxIndex].f1 = EsdDetector.cdfBoth(maxValue, tempNum);
			} else {
				break;
			}
		}

		if (detectLast) {
			return new Tuple3[] {results[results.length - 1]};
		} else {
			return results;
		}
	}

}
