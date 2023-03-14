package com.alibaba.alink.operator.common.outlier.tsa.tsacalculator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.operator.common.outlier.TimeSeriesAnomsUtils;
import com.alibaba.alink.params.outlier.tsa.TsaAlgoParams.ShortMoMAlgoParams;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class ShortMoMDetectorCalc extends PredictOutlierDetectorCalc {
	private static final long serialVersionUID = -1173292168357077056L;
	private int anomsNum;
	private double influence;
	//
	private PriorityQueue <Double> smallHeap = new PriorityQueue <>();
	private PriorityQueue <Double> largeHeap = new PriorityQueue <>();
	private double sum;
	private Double lastData;

	ShortMoMDetectorCalc() {}

	public ShortMoMDetectorCalc(Params params) {
		super(params);
		anomsNum = params.get(ShortMoMAlgoParams.ANOMS_NUM);
		influence = params.get(ShortMoMAlgoParams.INFLUENCE);
		if (anomsNum > trainNum) {
			throw new RuntimeException("the trainNum should be set larger than the anomaly number.");
		}
	}

	@Override
	public ShortMoMDetectorCalc clone() {
		ShortMoMDetectorCalc calc = new ShortMoMDetectorCalc();
		calc.trainNum = trainNum;
		calc.anomsNum = anomsNum;
		calc.influence = influence;
		if (smallHeap != null) {
			calc.smallHeap = new PriorityQueue <>();
			calc.smallHeap.addAll(smallHeap);
		}
		if (largeHeap != null) {
			calc.largeHeap = new PriorityQueue <>();
			calc.largeHeap.addAll(largeHeap);
		}
		calc.sum = sum;
		calc.lastData = lastData;
		return calc;
	}

	@Override
	public void reset() {
		smallHeap = new PriorityQueue <>();
		largeHeap = new PriorityQueue <>();
		sum = 0;
		lastData = null;
	}

	@Override
	public Tuple2 <Boolean, Double> predictBatchLast(double[] data) {
		int trainLength = data.length - 1;
		boolean isOutlier = false;
		double fittedData = data[trainLength];
		if (lastData == null) {
			for (int i = 0; i < trainLength; i++) {
				largeHeap.add(-data[i]);
				smallHeap.add(data[i]);
				sum += data[i];
			}
		} else {
			largeHeap.remove(-lastData);
			smallHeap.remove(lastData);
			largeHeap.add(-data[trainLength - 1]);
			smallHeap.add(data[trainLength - 1]);
			sum += (data[trainLength - 1] - lastData);
		}
		lastData = data[0];
		double max = -largeHeap.peek();
		double min = smallHeap.peek();
		double mean = sum / trainLength;
		double threshold = Math.min(max - mean, mean - min);
		int num = countNum(0, trainLength, threshold, data);
		if (num >= anomsNum) {
			fittedData = influence * data[trainLength] + (1 - influence) * data[trainLength - 1];
			isOutlier = true;
		}
		return Tuple2.of(isOutlier, fittedData);
	}

	@Override
	public int[] detectAndUpdateFormerData(double[] realData, double[] formerData, double[] predData) {
		int length = realData.length;
		List <Integer> anomsIndices = new ArrayList <>();
		for (int i = 0; i < length; i++) {
			double max = -largeHeap.peek();
			double min = smallHeap.peek();
			double mean = sum / length;
			double threshold = Math.min(max - mean, mean - min);
			int num = countNum(formerData, predData[i] - realData[i], threshold);
			if (num >= anomsNum) {
				if (i == 0) {
					predData[i] = influence * predData[i] + (1 - influence) * formerData[length - 1];
				} else {
					predData[i] = influence * predData[i] + (1 - influence) * predData[i - 1];
				}
				anomsIndices.add(i);
			}
			double detectData = predData[i] - realData[i];
			largeHeap.remove(-formerData[i]);
			smallHeap.remove(formerData[i]);
			largeHeap.add(-detectData);
			smallHeap.add(detectData);
			sum += detectData - formerData[i];
			formerData[i] = detectData;//直接将新的值写在formerData中。
		}
		return ArrayUtils.toPrimitive(anomsIndices.toArray(new Integer[0]));
	}

	@Override
	public void trainModel(double[] data) {
		for (double v : data) {
			largeHeap.add(-v);
			smallHeap.add(v);
			sum += v;
		}
	}

	@Override
	public SparseVector detect(double[] data) {
		return detectAnoms(data, trainNum, anomsNum, influence);
	}

	public static SparseVector detectAnoms(double[] data, int trainNum, int anomsNum, double influence) {
		List <Integer> indices = new ArrayList <>();

		PriorityQueue <Double> littleHeap = new PriorityQueue <>();
		PriorityQueue <Double> largeHeap = new PriorityQueue <>();
		littleHeap.add(data[0]);
		largeHeap.add(-data[0]);
		double sum = data[0];
		double threshold;
		int length = data.length;
		//对于初始的样本数目小于trainNum的，则将全部样本用于计算。
		int num;
		for (int i = 1; i < length - 1; i++) {
			if (i < trainNum) {
				largeHeap.add(-data[i]);
				littleHeap.add(data[i]);
				double max = -largeHeap.peek();
				double min = littleHeap.peek();
				sum += data[i];
				double mean = sum / (i + 1);
				threshold = Math.min(max - mean, mean - min);
				num = countNum(0, i + 1, threshold, data);
			} else {
				int preIndex = i - trainNum;
				littleHeap.remove(data[preIndex]);
				largeHeap.remove(-data[preIndex]);
				littleHeap.add(data[i]);
				largeHeap.add(-data[i]);
				double max = -largeHeap.peek();
				double min = littleHeap.peek();
				sum += (data[i] - data[preIndex]);
				double mean = sum / trainNum;
				threshold = Math.min(max - mean, mean - min);
				num = countNum(preIndex + 1, i + 1, threshold, data);
			}
			if (num >= anomsNum) {
				indices.add(i + 1);
				//修正异常值
				data[i + 1] = influence * data[i + 1] + (1 - influence) * data[i];
			}
		}
		SparseVector sv = TimeSeriesAnomsUtils.generateOutput(indices, data);
		return sv;
	}

	private static int countNum(int startIndex, int currentIndex, double threshold, double[] data) {
		int num = 0;
		for (int i = startIndex; i < currentIndex; i++) {
			if (Math.abs(data[currentIndex] - data[i]) > threshold) {
				num++;
			}
		}
		return num;
	}

	private static int countNum(double[] formerData, double currentData, double threshold) {
		int num = 0;
		int length = formerData.length;
		for (int i = 0; i < length; i++) {
			if (Math.abs(currentData - formerData[i]) > threshold) {
				num++;
			}
		}
		return num;
	}
}
