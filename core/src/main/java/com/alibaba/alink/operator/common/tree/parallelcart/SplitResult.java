package com.alibaba.alink.operator.common.tree.parallelcart;

import com.alibaba.alink.operator.common.tree.Criteria;

import java.io.Serializable;

class SplitResult implements Serializable {
	private int splitFeature;
	private int splitPoint;
	private int maxTreeId;
	private Criteria.MSE leftCounter;
	private Criteria.MSE rightCounter;
	private boolean[] featureCut;
	private boolean[] leftSubset;

	SplitResult() {
	}

	int getSplitFeature() {
		return splitFeature;
	}

	void setSplitFeature(int splitFeature) {
		this.splitFeature = splitFeature;
	}

	int getSplitPoint() {
		return splitPoint;
	}

	void setSplitPoint(int splitPoint) {
		this.splitPoint = splitPoint;
	}

	int getMaxTreeId() {
		return maxTreeId;
	}

	void setMaxTreeId(int maxTreeId) {
		this.maxTreeId = maxTreeId;
	}

	Criteria.MSE getLeftCounter() {
		return leftCounter;
	}

	void setLeftCounter(Criteria.MSE leftCounter) {
		this.leftCounter = leftCounter;
	}

	Criteria.MSE getRightCounter() {
		return rightCounter;
	}

	void setRightCounter(Criteria.MSE rightCounter) {
		this.rightCounter = rightCounter;
	}

	boolean[] getFeatureCut() {
		return featureCut;
	}

	void setFeatureCut(boolean[] featureCut) {
		this.featureCut = featureCut;
	}

	boolean[] getLeftSubset() {
		return leftSubset;
	}

	void setLeftSubset(boolean[] leftSubset) {
		this.leftSubset = leftSubset;
	}
}
