package com.alibaba.alink.operator.common.distance;

import com.alibaba.alink.common.linalg.*;


public class InnerProduct extends FastDistance {
    @Override
    public double calc(double[] array1, double[] array2) {
        return BLAS.dot(array1, array2);
    }

    @Override
    public double calc(Vector vec1, Vector vec2) {
        return MatVecOp.dot(vec1, vec2);
    }

    @Override
    public void updateLabel(FastDistanceData data) {

    }

    @Override
    double calc(FastDistanceVectorData left, FastDistanceVectorData right) {
        return MatVecOp.dot(left.vector, right.vector);
    }

    @Override
    void calc(FastDistanceVectorData leftVector, FastDistanceMatrixData rightVectors, double[] res) {
        CosineDistance.baseCalc(leftVector, rightVectors, res, 0, 1);
    }

    @Override
    void calc(FastDistanceMatrixData left, FastDistanceMatrixData right, DenseMatrix res) {
        CosineDistance.baseCalc(left, right, res, 0, 1);
    }

    @Override
    void calc(FastDistanceVectorData left, FastDistanceSparseData right, double[] res) {
        CosineDistance.baseCalc(left, right, res, 0, x -> x);
    }

    @Override
    void calc(FastDistanceSparseData left, FastDistanceSparseData right, double[] res) {
        CosineDistance.baseCalc(left, right, res, 0, x -> x);
    }

}
