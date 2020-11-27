package com.alibaba.alink.operator.common.tree;

import java.io.Serializable;

/**
 * FeatureMeta.
 */
public class FeatureMeta implements Serializable {
	private static final long serialVersionUID = 645879023264618004L;

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
	private int sparseZeroIndex;
	private int missingIndex;

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

	public FeatureMeta(
		String name,
		int index,
		FeatureType featureType,
		int numCategorical,
		int sparseZeroIndex,
		int missingIndex) {

		this.name = name;
		this.type = featureType;
		this.index = index;

		this.numCategorical = numCategorical;
		this.sparseZeroIndex = sparseZeroIndex;
		this.missingIndex = missingIndex;
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

	public int getSparseZeroIndex() {
		return sparseZeroIndex;
	}

	public int getMissingIndex() {
		return missingIndex;
	}

	public void rewrite(int numCategorical, int sparseZeroIndex, int missingIndex) {
		this.numCategorical = numCategorical;
		this.sparseZeroIndex = sparseZeroIndex;
		this.missingIndex = missingIndex;
	}

	@Override
	public String toString() {
		return "name:" + this.name + "; index:" + this.index + "; featureType:" + this.type.toString()
			+ "; numCategorical:"
			+ this.numCategorical + "; sparseZeroIndex:" + this.sparseZeroIndex + "; missingIndex:" + this
			.missingIndex;
	}
}
