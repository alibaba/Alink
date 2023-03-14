package com.alibaba.alink.operator.common.outlier.tsa.tsacalculator;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.outlier.CalcMidian;
import com.alibaba.alink.operator.common.outlier.TimeSeriesAnomsUtils;
import com.alibaba.alink.params.outlier.HasDirection;
import com.alibaba.alink.params.outlier.tsa.HasMaxAnoms;
import com.alibaba.alink.params.outlier.tsa.HasSHESDAlpha;
import com.alibaba.alink.params.outlier.tsa.TsaAlgoParams.SHESDAlgoParams;

import java.util.HashSet;

public class SHESDDetectorCalc extends DecomposeOutlierDetectorCalc {
	private static final long serialVersionUID = 8617646058413698191L;
	private double maxAnoms;
	private double alpha;
	private HasDirection.Direction direction;

	private int frequency;

	public SHESDDetectorCalc(Params params) {
		maxAnoms = params.get(HasMaxAnoms.MAX_ANOMS);
		alpha = params.get(HasSHESDAlpha.SHESD_ALPHA);
		direction = params.get(HasDirection.DIRECTION);
		frequency = params.get(SHESDAlgoParams.FREQUENCY);
	}

	@Override
	public int[] detect(double[] data) {
		return detect(data, frequency, maxAnoms, alpha, direction);
	}

	public static int[] detect(double[] data, int frequency, double maxAnoms, double alpha,
							   HasDirection.Direction direction) {
		DenseVector[] components = STLDecomposerCalc.decompose(data, frequency);
		return shesdMethod(data, components, maxAnoms, alpha, direction);
	}

	//the indices of sv is the indices of anomalies data, and value is the reminder.
	public static int[] shesdMethod(double[] data, DenseVector[] components,
									double maxAnoms, double alpha,
									HasDirection.Direction direction) {
		double[] trend = components[0].getData();
		double[] seasonal = components[1].getData();
		int dataNum = data.length;
		double dataMedian = CalcMidian.tempMedian(data);
		double[] dataDecomp = new double[dataNum];
		//here minus median and seasonal of data.
		for (int i = 0; i < dataNum; i++) {
			data[i] -= (dataMedian + seasonal[i]);
			dataDecomp[i] = trend[i] + seasonal[i];
		}
		CalcMidian calcMidian = new CalcMidian(data);
		int maxOutliers = (int) Math.floor(dataNum * maxAnoms);
		int[] outlierIndex = new int[maxOutliers];
		double[] ares = new double[dataNum];
		HashSet <Integer> excludedIndices = new HashSet <>();
		int numAnoms = 0;
		for (int i = 1; i < maxOutliers + 1; i++) {
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
				numAnoms = i;
			}
		}
		if (numAnoms == 0) {
			return new int[0];
		} else {
			int[] indices = new int[numAnoms];
			System.arraycopy(outlierIndex, 0, indices, 0, numAnoms);
			return indices;
		}
	}
}
