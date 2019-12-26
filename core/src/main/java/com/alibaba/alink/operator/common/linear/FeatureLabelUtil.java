package com.alibaba.alink.operator.common.linear;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

/**
 * Util class for the operations on feature cols and labels in linear algo.
 */
public class FeatureLabelUtil {

    /**
     *  Retrieve the feature vector from the input row data.
     */
    public static Vector getFeatureVector(Row row, boolean hasInterceptItem, int featureN,
                                          int[] featureIdx,
                                          int vectorColIndex, Integer vectorSize) {
        Vector aVector;
        if (vectorColIndex != -1) {
            Vector vec = VectorUtil.getVector(row.getField(vectorColIndex));
            if (vec instanceof SparseVector) {
                SparseVector tmp = (SparseVector)vec;
                if (null != vectorSize) {
                    tmp.setSize(vectorSize);
                }
                aVector = hasInterceptItem ? tmp.prefix(1.0) : tmp;
            } else {
                DenseVector tmp = (DenseVector)vec;
                aVector = hasInterceptItem ? tmp.prefix(1.0) : tmp;
            }
        } else {
            if (hasInterceptItem) {
                aVector = new DenseVector(featureN + 1);
                aVector.set(0, 1.0);
                for (int i = 0; i < featureN; i++) {
                    if (row.getField(featureIdx[i]) instanceof Number) {
                        aVector.set(i + 1, ((Number)row.getField(featureIdx[i])).doubleValue());
                    }
                }
            } else {
                aVector = new DenseVector(featureN);
                for (int i = 0; i < featureN; i++) {
                    if (row.getField(featureIdx[i]) instanceof Number) {
                        aVector.set(i, ((Number)row.getField(featureIdx[i])).doubleValue());
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
            return ((Number)row.getField(labelColIndex)).doubleValue();
        } else {
            return row.getField(labelColIndex).toString().equals(positiveLableValueString) ? 1.0 : -1.0;
        }
    }

    /**
     * After jsonized and un-jsonized, the label type may be changed.
     * So here we recover the label type.
     */
    static Object[] recoverLabelType(Object[] labels, TypeInformation labelType) {

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
     * Get the weight value.
     */
    public static double getWeightValue(Row row, int weightColIndex) {
        return weightColIndex >= 0 ? ((Number)row.getField(weightColIndex)).doubleValue() : 1.0;
    }
}
