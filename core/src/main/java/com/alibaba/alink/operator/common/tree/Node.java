package com.alibaba.alink.operator.common.tree;

import java.io.Serializable;

/**
 * Tree node in the decision tree that will be serialized to json and deserialized from json.
 */
public class Node implements Serializable {
	private static final long serialVersionUID = 1788232094688921790L;
	/**
	 * featureIndex == -1 using for leaf
	 */
	private int featureIndex;

	/**
	 * gain for split
	 */
	private double gain;

	/*
	 * information of node
	 */
	private LabelCounter counter;

	/**
	 * categorical split point
	 * indicate the branchId of child
	 */
	private int[] categoricalSplit;

	/**
	 * continuous split point
	 */
	private double continuousSplit;

	/**
	 * indicate the branchId of missing value.
	 * weight confidence by default(== null).
	 */
	private int[] missingSplit;

	/**
	 * for mem read&write
	 */
	private transient Node[] nextNodes;

	public Node() {
		featureIndex = -1;
	}

	public Node copy(Node node) {
		featureIndex = node.featureIndex;
		gain = node.gain;
		counter = node.counter;
		categoricalSplit = node.categoricalSplit;
		continuousSplit = node.continuousSplit;
		missingSplit = node.missingSplit;
		nextNodes = node.nextNodes;

		return this;
	}

	public boolean isLeaf() {
		return featureIndex == -1;
	}

	public Node[] getNextNodes() {
		return nextNodes;
	}

	public Node setNextNodes(Node[] nextNodes) {
		this.nextNodes = nextNodes;
		return this;
	}

	public int getFeatureIndex() {
		return featureIndex;
	}

	public Node setFeatureIndex(int featureIndex) {
		this.featureIndex = featureIndex;
		return this;
	}

	public Node makeLeaf() {
		featureIndex = -1;
		gain = 0.0;
		categoricalSplit = null;
		continuousSplit = 0.0;
		nextNodes = null;
		return this;
	}

	public Node makeLeafProb() {
		counter.normWithWeight();
		return this;
	}

	public double getGain() {
		return gain;
	}

	public Node setGain(double gain) {
		this.gain = gain;
		return this;
	}

	public LabelCounter getCounter() {
		return counter;
	}

	public Node setCounter(LabelCounter counter) {
		this.counter = counter;
		return this;
	}

	public int[] getCategoricalSplit() {
		return categoricalSplit;
	}

	public Node setCategoricalSplit(int[] categoricalSplit) {
		this.categoricalSplit = categoricalSplit;
		return this;
	}

	public double getContinuousSplit() {
		return continuousSplit;
	}

	public Node setContinuousSplit(double continuousSplit) {
		this.continuousSplit = continuousSplit;
		return this;
	}

	public int[] getMissingSplit() {
		return missingSplit;
	}

	public Node setMissingSplit(int[] missingSplit) {
		this.missingSplit = missingSplit;
		return this;
	}
}
