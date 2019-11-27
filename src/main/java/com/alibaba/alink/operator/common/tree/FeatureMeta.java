package com.alibaba.alink.operator.common.tree;

import java.io.Serializable;

/**
 * FeatureMeta.
 */
public class FeatureMeta implements Serializable {
	public enum FeatureType implements Serializable {

		/**
		 * Feature of categorical value, such as int, string, etc.
		 */
		CATEGORICAL,

		/**
		 * Feature of continuous value, such as double.
		 */
		CONTINUOUS
	}

	private String name;
	private FeatureType type;
	private int index;
	private int numCategorical;

	public FeatureMeta() {
	}

	public FeatureMeta(String name, int index) {
		this.name = name;
		this.type = FeatureType.CONTINUOUS;
		this.index = index;
	}

	public FeatureMeta(String name, int index, int numCategorical) {
		this.name = name;
		this.type = FeatureType.CATEGORICAL;
		this.index = index;
		this.numCategorical = numCategorical;
	}

	public FeatureType getType() {
		return type;
	}

	public String getName() {
		return name;
	}

	public int getIndex() {
		return index;
	}

	public int getNumCategorical() {
		return numCategorical;
	}
}
