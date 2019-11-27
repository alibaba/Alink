package com.alibaba.alink.operator.common.distance;

import com.alibaba.alink.common.linalg.*;

/**
 * ManHattan Distance of two points is the sum of the absolute differences of their Cartesian coordinates.
 * <p>
 * https://en.wikipedia.org/wiki/Taxicab_geometry
 * <p>
 * ManHatton Distance is also known as L1 distance, city block distance, taxicab distance.
 * <p>
 * Given two vectors a and b, ManHattan Distance = |a - b|_1, where |*|_1 means the the sum of the absolute differences.
 */
public class ManHattanDistance extends FastDistance{
    @Override
    public double calc(double[] vec1, double[] vec2) {
        return calc(vec1, 0, vec2, 0, vec1.length);
    }

    @Override
    public double calc(Vector vec1, Vector vec2) {
        return MatVecOp.sumAbsDiff(vec1, vec2);
    }

    private static double calc(double[] data1, int start1, double[] data2, int start2, int len){
        double sum = 0.;
        for(int i = 0; i < len; i++){
            sum += Math.abs(data1[start1 + i] - data2[start2 + i]);
        }
        return sum;
    }

    @Override
    public void updateLabel(FastDistanceData data){
    }

    @Override
    double calc(FastDistanceVectorData left, FastDistanceVectorData right){
        return calc(left.vector, right.vector);
    }

    @Override
    void calc(FastDistanceVectorData vector, FastDistanceMatrixData matrix, double[] res){
        Vector vec = vector.getVector();
        if(vec instanceof DenseVector){
            double[] vecData = ((DenseVector)vec).getData();
            double[] matrixData = matrix.getVectors().getData();
            int vectorSize = vecData.length;
            for(int i = 0; i < matrix.getVectors().numCols(); i++){
                res[i] = calc(vecData, 0, matrixData, i * vectorSize, vectorSize);
            }
        }else{
            int[] indices = ((SparseVector)vec).getIndices();
            double[] values = ((SparseVector)vec).getValues();
            DenseMatrix denseMatrix = matrix.getVectors();
            double[] matrixData = denseMatrix.getData();
            int cnt = 0;
            for(int i = 0; i < denseMatrix.numCols(); i++){
                int p1 = 0;
                for (int j = 0; j < denseMatrix.numRows(); j++) {
                    if (p1 < indices.length && indices[p1] == j) {
                        res[i] += Math.abs(values[p1] - matrixData[cnt++]);
                        p1++;
                    } else {
                        res[i] += Math.abs(matrixData[cnt++]);
                    }
                }
            }
        }
    }

    @Override
    void calc(FastDistanceMatrixData left, FastDistanceMatrixData right, DenseMatrix res){
        int vectorSize = right.vectors.numRows();
        int cnt = 0;
        int leftCnt = 0;
        double[] resData = res.getData();
        for(int i = 0; i < res.numCols(); i++){
            int rightCnt = 0;
            for(int j = 0; j < res.numRows(); j++){
                resData[cnt++] = calc(right.vectors.getData(), rightCnt, left.vectors.getData(), leftCnt, vectorSize);
                rightCnt += vectorSize;
            }
            leftCnt += vectorSize;
        }
    }
}
