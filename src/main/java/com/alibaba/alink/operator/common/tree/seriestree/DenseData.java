package com.alibaba.alink.operator.common.tree.seriestree;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.tree.FeatureMeta;

/**
 * DataSet.
 */
public class DenseData {
	private final static double CONTINUOUS_NULL = Double.NaN;
	private final static int CATEGORICAL_NULL = Integer.MAX_VALUE;

	/**
	 * Number of instances.
	 */
	int m;

	/**
	 * Number of feature columns.
	 */
	int n;

	/**
	 * Features.
	 */
	FeatureMeta[] featureMetas;
	private Object[] featureValues;

	FeatureMeta labelMeta;
	private Object labelValues;

	double[] weights;

	public DenseData(
		int m, FeatureMeta[] featureMetas, FeatureMeta labelMeta) {
		this.m = m;
		this.n = featureMetas.length;
		this.featureMetas = featureMetas;

		featureValues = new Object[n];
		for (int i = 0;i < n; ++i) {
			if (this.featureMetas[i].getType() == FeatureMeta.FeatureType.CONTINUOUS) {
				featureValues[i] = new double[m];
			} else {
				featureValues[i] = new int[m];
			}
		}

		weights = new double[m];

		if (labelMeta != null) {
			this.labelMeta = labelMeta;

			if (this.labelMeta.getType() == FeatureMeta.FeatureType.CONTINUOUS) {
				labelValues = new double[m];
			} else {
				labelValues = new int[m];
			}
		}

	}

	public <T> T getFeatureValues(int index) {
		return (T) featureValues[index];
	}

	public <T> T getLabelValues() {
		return (T) labelValues;
	}

	public void readFromInstances(Iterable<Row> instances) {
		int i = 0;
		for (Row instance : instances) {
			//initial feature values.
			for (int j = 0; j < n; ++j) {
				if (featureMetas[j].getType() == FeatureMeta.FeatureType.CONTINUOUS) {
					double[] continuousFeatureValues = getFeatureValues(j);
					if (instance.getField(j) == null) {
						continuousFeatureValues[i] = CONTINUOUS_NULL;
					} else {
						continuousFeatureValues[i] = (double) instance.getField(j);
					}
				} else {
					int[] categoricalFeatureValues = getFeatureValues(j);
					if (instance.getField(j) == null) {
						categoricalFeatureValues[i] = CATEGORICAL_NULL;
					} else {
						categoricalFeatureValues[i] = (int) instance.getField(j);
					}
				}
			}

			// initial label values.
			if (labelMeta != null) {
				if (labelMeta.getType() == FeatureMeta.FeatureType.CONTINUOUS) {
					double[] continuousLabelValues = getLabelValues();
					continuousLabelValues[i] = (double) instance.getField(n);
				} else {
					int[] categoricalLabelValues = getLabelValues();
					categoricalLabelValues[i] = (int) instance.getField(n);
				}
			}

			// initial weights.
			if (labelMeta != null && instance.getArity() == n + 2
				|| labelMeta == null && instance.getArity() == n + 1) {
				weights[i] = (double) instance.getField(n + 1);
			} else {
				weights[i] = 1.0;
			}

			i++;
		}
	}

	public static boolean isContinuousMissValue(double value) {
		return Double.isNaN(value);
	}

	public static boolean isCategoricalMissValue(int value) {
		return value == CATEGORICAL_NULL;
	}
}
