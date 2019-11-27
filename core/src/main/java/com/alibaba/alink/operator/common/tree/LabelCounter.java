package com.alibaba.alink.operator.common.tree;

import java.io.Serializable;

public class LabelCounter implements Serializable {
	private double weightSum;
	private int numInst;
	private double[] distributions;

	public LabelCounter() {
	}

	public LabelCounter(
		double weightSum,
		int numInst,
		double[] distributions) {
		this.weightSum = weightSum;
		this.numInst = numInst;
		this.distributions = distributions;
	}

	public double getWeightSum() {
		return weightSum;
	}

	public int getNumInst() {
		return numInst;
	}

	public double[] getDistributions() {
		return distributions;
	}

	public LabelCounter add(LabelCounter other, double weight) {
		this.weightSum += weight;

		if (distributions != null) {
			for (int i = 0; i < distributions.length; ++i) {
				distributions[i] += other.distributions[i] * weight;
			}
		}

		return this;
	}

	public LabelCounter normWithWeight() {
		if (weightSum == 0.) {
			return this;
		}

		if (distributions != null) {
			for (int i = 0; i < distributions.length; ++i) {
				distributions[i] /= weightSum;
			}
		}

		return this;
	}
}
