package com.alibaba.alink.operator.common.outlier;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

public class CalcMidian {
	private PriorityQueue <Double> maxHeap = new PriorityQueue <>(new Comparator <Double>() {
		@Override
		public int compare(Double o1, Double o2) {
			double value = o2 - o1;
			if (value > 0) {
				return 1;
			} else if (value < 0) {
				return -1;
			} else {
				return 0;
			}
		}
	});

	private PriorityQueue <Double> minHeap = new PriorityQueue <>();

	public CalcMidian() {}

	public CalcMidian(double[] data) {
		for (double datum : data) {
			add(datum);
		}
	}

	public void add(double value) {
		int minSize = minHeap.size();
		int maxSize = maxHeap.size();
		if (maxSize == 0) {
			maxHeap.add(value);
			return;
		}
		if (minSize == 0) {
			minHeap.add(value);
			return;
		}
		double smallerValue = maxHeap.peek();
		double largerValue = minHeap.peek();
		if (maxSize <= minSize) {
			//add in the maxHeap
			if (value <= largerValue) {
				maxHeap.add(value);
			} else {
				double tempValue = minHeap.poll();
				maxHeap.add(tempValue);
				minHeap.add(value);
			}
		} else {
			//add in the minHeap
			if (value >= smallerValue) {
				minHeap.add(value);
			} else {
				double tempValue = maxHeap.poll();
				minHeap.add(tempValue);
				maxHeap.add(value);
			}
		}
	}

	public double median() {
		int minSize = minHeap.size();
		int maxSize = maxHeap.size();
		if (minSize == 0 && maxSize == 0) {
			throw new RuntimeException("there is no data");
		}
		if (minSize == maxSize) {
			return (maxHeap.peek() + minHeap.peek()) / 2;
		}
		if (minSize > maxSize) {
			return minHeap.peek();
		}
		return maxHeap.peek();
	}

	public boolean remove(double value) {
		boolean minContain = minHeap.contains(value);
		boolean maxContain = maxHeap.contains(value);
		if (!(minContain || maxContain)) {
			return false;
		}
		//may have multiple value, just remove one.
		if (maxContain) {
			maxHeap.remove(value);
			if (maxHeap.size() < minHeap.size() - 1) {
				double tempValue = minHeap.poll();
				maxHeap.add(tempValue);
			}
		} else {
			minHeap.remove(value);
			if (minHeap.size() < maxHeap.size() - 1) {
				double tempValue = maxHeap.poll();
				minHeap.add(tempValue);
			}
		}
		return true;
	}

	public double absMedian(double c) {
		double median = this.median();
		int minHeapLength = minHeap.size();
		int maxHeapLength = maxHeap.size();
		double[] dataArray = new double[minHeapLength + maxHeapLength];
		int maxCount = maxHeapLength - 1;
		Iterator <Double> maxIter = maxHeap.iterator();
		while (maxCount >= 0) {
			dataArray[maxCount] = Math.abs(maxIter.next() - median);
			maxCount--;
		}
		int minCount = maxHeapLength;
		Iterator <Double> minIter = minHeap.iterator();
		while (minCount < minHeapLength + maxHeapLength) {
			dataArray[minCount] = Math.abs(minIter.next() - median);
			minCount++;
		}
		return CalcMidian.tempMedian(dataArray) / c;
	}

	public static double tempMedian(double[] dataOrigin) {
		int length = dataOrigin.length;
		if (length == 0) {
			return 0;
		}
		double[] data = dataOrigin.clone();
		Arrays.sort(data);
		if (length % 2 == 1) {
			return data[length >> 1];
		}
		return (data[length >> 1] + data[(length >> 1) - 1]) / 2;
	}
}
