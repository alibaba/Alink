package com.alibaba.alink.operator.common.linear;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;

/**
 * Util class for the operations on feature cols and labels in linear algo.
 */
public class FeatureLabelUtil {

	public static Vector getVectorFeature(Object input, boolean hasInterceptItem, Integer vectorSize) {
		Vector aVector;
		Vector vec = VectorUtil.getVector(input);
		if (vec instanceof SparseVector) {
			SparseVector tmp = (SparseVector) vec;
			if (null != vectorSize && tmp.size() > 0) {
				tmp.setSize(vectorSize);
			}
			aVector = hasInterceptItem ? tmp.prefix(1.0) : tmp;
		} else {
			DenseVector tmp = (DenseVector) vec;
			aVector = hasInterceptItem ? tmp.prefix(1.0) : tmp;
		}
		return aVector;
	}

	/**
	 * Retrieve the feature vector from the input row data.
	 */
	public static Vector getTableFeature(Row row, boolean hasInterceptItem, int featureN,
										 int[] featureIdx) {
		Vector aVector;
		if (hasInterceptItem) {
			aVector = new DenseVector(featureN + 1);
			aVector.set(0, 1.0);
			for (int i = 0; i < featureN; i++) {
				if (row.getField(featureIdx[i]) instanceof Number) {
					aVector.set(i + 1, ((Number) row.getField(featureIdx[i])).doubleValue());
				}
			}
		} else {
			aVector = new DenseVector(featureN);
			for (int i = 0; i < featureN; i++) {
				if (row.getField(featureIdx[i]) instanceof Number) {
					aVector.set(i, ((Number) row.getField(featureIdx[i])).doubleValue());
				}
			}
		}
		return aVector;
	}

	/**
	 * Retrieve the feature vector from the input row data.
	 */
	public static Vector getFeatureVector(Row row, boolean hasInterceptItem, int featureN,
										  int[] featureIdx,
										  int vectorColIndex, Integer vectorSize) {
		Vector aVector;
		if (vectorColIndex != -1) {
			Vector vec = VectorUtil.getVector(row.getField(vectorColIndex));
			if (vec instanceof SparseVector) {
				SparseVector tmp = (SparseVector) vec;
				if (null != vectorSize && tmp.size() > 0) {
					tmp.setSize(vectorSize);
				}
				aVector = hasInterceptItem ? tmp.prefix(1.0) : tmp;
			} else {
				DenseVector tmp = (DenseVector) vec;
				aVector = hasInterceptItem ? tmp.prefix(1.0) : tmp;
			}
		} else {
			if (hasInterceptItem) {
				aVector = new DenseVector(featureN + 1);
				aVector.set(0, 1.0);
				for (int i = 0; i < featureN; i++) {
					if (row.getField(featureIdx[i]) instanceof Number) {
						aVector.set(i + 1, ((Number) row.getField(featureIdx[i])).doubleValue());
					}
				}
			} else {
				aVector = new DenseVector(featureN);
				for (int i = 0; i < featureN; i++) {
					if (row.getField(featureIdx[i]) instanceof Number) {
						aVector.set(i, ((Number) row.getField(featureIdx[i])).doubleValue());
					}
				}
			}
		}
		return aVector;
	}

	/**
	 * Get the label value from the input row data.
	 */
	public static double getLabelValue(Row row, boolean isRegProc, int labelColIndex, String
		positiveLableValueString) {
		if (isRegProc) {
			return ((Number) row.getField(labelColIndex)).doubleValue();
		} else {
			return row.getField(labelColIndex).toString().equals(positiveLableValueString) ? 1.0 : -1.0;
		}
	}

	/**
	 * After jsonized and un-jsonized, the label type may be changed. So here we recover the label type.
	 */
	public static Object[] recoverLabelType(Object[] labels, TypeInformation labelType) {

		if (labels == null) {
			return null;
		}

		for (int i = 0; i < labels.length; i++) {
			Object label = labels[i];
			if (label == null) {
				continue;
			}
			if (label instanceof String) {
				String strLable = (String) label;
				try {
					LabelTypeEnum.StringTypeEnum operation =
						LabelTypeEnum.StringTypeEnum.valueOf(labelType.toString().toUpperCase());
					labels[i] = operation.getOperation().apply(strLable);
				} catch (Exception e) {
					throw new RuntimeException("unknown label type: " + labelType);
				}
			} else if (label instanceof Double) {
				Double dLabel = (Double) label;
				LabelTypeEnum.DoubleTypeEnum operation =
					LabelTypeEnum.DoubleTypeEnum.valueOf(labelType.toString().toUpperCase());
				labels[i] = operation.getOperation().apply(dLabel);
			}
		}
		return labels;
	}

	/**
	 * Compute vec1 \cdot vec2 for linear model mapper specially.
	 */
	public static double dot(Vector vec1, DenseVector vec2) {
		if (vec1 instanceof DenseVector) {
			if (vec1.size() == vec2.size()) {
				return BLAS.dot((DenseVector) vec1, vec2);
			} else {
				double ret = 0.;
				int size = Math.min(vec1.size(), vec2.size());
				for (int i = 0; i < size; ++i) {
					ret += vec1.get(i) * vec2.get(i);
				}
				return ret;
			}
		} else {
			double[] values = ((SparseVector) vec1).getValues();
			int[] indices = ((SparseVector) vec1).getIndices();
			double s = 0.;
			for (int i = 0; i < indices.length; i++) {
				if (indices[i] < vec2.size()) {
					s += values[i] * vec2.get(indices[i]);
				}
			}
			return s;
		}
	}

	/**
	 * Get the weight value.
	 */
	public static double getWeightValue(Row row, int weightColIndex) {
		return weightColIndex >= 0 ? ((Number) row.getField(weightColIndex)).doubleValue() : 1.0;
	}
}
