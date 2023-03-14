package com.alibaba.alink.operator.common.outlier.tsa.tsacalculator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.params.outlier.tsa.TsaAlgoParams.SmoothZScoreAlgoParams;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.List;

public class SmoothZScoreDetectorCalc extends PredictOutlierDetectorCalc {
	private static final long serialVersionUID = -1510748793393605203L;
	private double threshold;
	private double influence;
	//辅助计算，减少计算量。
	private double lastSum;
	private double lastSquareSum;
	private Double lastData;

	@Override
	public SmoothZScoreDetectorCalc clone() {
		SmoothZScoreDetectorCalc calc = new SmoothZScoreDetectorCalc();
		calc.trainNum = trainNum;
		calc.threshold = threshold;
		calc.influence = influence;
		calc.lastSum = lastSum;
		calc.lastSquareSum = lastSquareSum;
		calc.lastData = lastData;
		return calc;
	}

	@Override
	public void reset() {
		this.lastSum = 0;
		this.lastSquareSum = 0;
		this.lastData = null;
	}

	SmoothZScoreDetectorCalc() {}

	public SmoothZScoreDetectorCalc(Params params) {
		super(params);
		threshold = params.get(SmoothZScoreAlgoParams.THRESHOLD);
		influence = params.get(SmoothZScoreAlgoParams.INFLUENCE);
	}

	@Override
	public SparseVector detect(double[] data) {
		return calcZScore(data, trainNum, threshold, influence, false);
	}

	@Override
	public void trainModel(double[] data) {
		double sum = 0;
		double squareSum = 0;
		for (double datum : data) {
			sum += datum;
			squareSum += Math.pow(datum, 2);
		}
		lastSum = sum;
		lastSquareSum = squareSum;
	}

	//detect last just use all to detect the last one.
	//train the initial model with the first trainNum data.
	public static SparseVector calcZScore(double[] data, int trainNum, double threshold, double influence,
										  boolean detectLast) {
		if (detectLast) {
			int length = data.length - 1;
			double sum = 0;
			double squareSum = 0;
			for (int i = 0; i < length; i++) {
				sum += data[i];
				squareSum += Math.pow(data[i], 2);
			}
			double avg = sum / length;
			double std = Math.sqrt(squareSum / length - Math.pow(avg, 2));

			if (Math.abs((data[length] - avg)) > threshold * std) {
				return new SparseVector(1, new int[] {0}, new double[] {0});
			} else {
				return new SparseVector(0);
			}
		} else {
			if (data.length <= trainNum) {
				return new SparseVector();
			}
			Tuple2 <double[], ArrayList <Integer>> res = analyzeDataForSignals(data, trainNum, threshold, influence);
			int length = res.f1.size();
			int[] indices = new int[length];
			double[] values = new double[length];
			for (int i = 0; i < length; i++) {
				indices[i] = res.f1.get(i);
				values[i] = data[indices[i]];
			}
			return new SparseVector(data.length, indices, values);
		}
	}

	@Override
	public Tuple2 <Boolean, Double> predictBatchLast(double[] data) {
		int trainLength = data.length - 1;
		double avg;
		double std;
		boolean isOutlier = false;
		double fittedData = data[trainLength];
		if (lastData == null) {
			double sum = 0;
			double squareSum = 0;
			for (int i = 0; i < trainLength; i++) {
				sum += data[i];
				squareSum += Math.pow(data[i], 2);
			}
			lastSum = sum;
			lastSquareSum = squareSum;
		} else {
			lastSum += (data[trainLength - 1] - lastData);
			lastSquareSum += (Math.pow(data[trainLength - 1], 2) - Math.pow(lastData, 2));
		}
		lastData = data[0];
		avg = lastSum / trainLength;
		std = Math.sqrt(lastSquareSum / trainLength - Math.pow(avg, 2));
		if (Math.abs((data[trainLength] - avg)) > threshold * std) {
			fittedData = influence * data[trainLength] + (1 - influence) * data[trainLength - 1];
			isOutlier = true;
		}
		return Tuple2.of(isOutlier, fittedData);
	}

	@Override
	public int[] detectAndUpdateFormerData(double[] realData, double[] formerData, double[] predData) {
		int predictNum = realData.length;

		List <Integer> anomsIndices = new ArrayList <>();
		for (int i = 0; i < predictNum; i++) {
			double avg = lastSum / predictNum;
			double std = Math.sqrt(lastSquareSum / predictNum - Math.pow(avg, 2));
			double detectData = predData[i] - realData[i];
			if (Math.abs((detectData - avg)) > threshold * std) {
				if (i == 0) {
					predData[i] = influence * predData[i] + (1 - influence) * formerData[predictNum - 1];
				} else {
					predData[i] = influence * predData[i] + (1 - influence) * predData[i - 1];
				}
				detectData = predData[i] - realData[i];
				anomsIndices.add(i);
			}
			lastSum += detectData - formerData[i];
			lastSquareSum += Math.pow(detectData, 2) - Math.pow(formerData[i], 2);
			//构造新的former。被替换成了新一轮预测中的残差数据。
			formerData[i] = predData[i] - realData[i];
		}
		return ArrayUtils.toPrimitive(anomsIndices.toArray(new Integer[0]));
	}

	//返回拟合了的数据以及异常点的id。
	public static Tuple2 <double[], ArrayList <Integer>> analyzeDataForSignals(double[] data, int trainNum,
																			   double threshold, double influence) {

		// the results of our algorithm
		ArrayList <Integer> signals = new ArrayList <>();

		// filter out the signals (peaks) from our original list (using influence arg)
		double[] filteredData = data.clone();//the initial data (which is in the window count) is important.
		// init avgFilter and stdFilter
		double sum = 0;
		double squareSum = 0;
		for (int i = 0; i < trainNum; i++) {
			sum += filteredData[i];
			squareSum += Math.pow(filteredData[i], 2);
		}

		double avg = sum / trainNum;
		double std = Math.sqrt(squareSum / trainNum - Math.pow(avg, 2));

		// loop input starting at end of rolling window
		for (int i = trainNum + 1; i < data.length; i++) {
			// if the distance between the current value and average is enough standard deviations (threshold) away
			if (Math.abs((data[i] - avg)) > threshold * std) {
				// this is a signal (i.e. peak), determine if it is a positive or negative signal
				signals.add(i);
				// filter this signal out using influence
				filteredData[i] = (influence * data[i]) + ((1 - influence) * filteredData[i - 1]);
			} else {
				// ensure this value is not filtered
				filteredData[i] = data[i];
			}
			sum -= filteredData[i - trainNum];
			sum += filteredData[i];
			squareSum -= Math.pow(filteredData[i - trainNum], 2);
			squareSum += Math.pow(filteredData[i], 2);

			avg = sum / trainNum;
			std = Math.sqrt(squareSum / trainNum - Math.pow(avg, 2));
		}

		return Tuple2.of(filteredData, signals);
	} // end
}
