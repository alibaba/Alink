package com.alibaba.alink.operator.common.distance;

import com.alibaba.alink.common.linalg.*;

import java.util.Arrays;

/**
 * Cosine similarity is a measure of similarity between two non-zero vectors of an inner product space that measures the
 * cosine of the angle between them.
 * <p>
 * https://en.wikipedia.org/wiki/Cosine_similarity
 * <p>
 * Given two vectors a and b, cosine similarity = (a * b / ||a|| / ||b||), where a * b means dot product, ||a|| means
 * the L2 norm of the vector.
 * <p>
 * Cosine Distance is often used for the complement in positive space, cosine distance = 1 - cosine similarity.
 */
public class CosineDistance extends FastDistance {
    /**
     * Label size.
     */
    private static int LABEL_SIZE = 1;

    /**
     * Calculate the Cosine distance between two arrays.
     *
     * @param array1 array1
     * @param array2 array2
     * @return the distance
     */
    @Override
    public double calc(double[] array1, double[] array2) {
        double dot = BLAS.dot(array1, array2);
        double cross = Math.sqrt(BLAS.dot(array1, array1) * BLAS.dot(array2, array2));
        return 1.0 - (cross > 0.0 ? dot / cross : 0.0);
    }

    /**
     * Calculate the distance between vec1 and vec2.
     *
     * @param vec1 vector1
     * @param vec2 vector2
     * @return the distance.
     */
    @Override
    public double calc(Vector vec1, Vector vec2) {
        double dot = MatVecOp.dot(vec1, vec2);
        double cross = vec1.normL2() * vec2.normL2();
        return 1.0 - (cross > 0.0 ? dot / cross : 0.0);
    }

    /**
     * For Cosine distance, distance = 1 - a * b / ||a|| / ||b|| So we can tranform the vector the unit vector, and when
     * we need to calculate the distance with another vector, only dot product is calculated.
     *
     * @param data FastDistanceData.
     */
    @Override
    public void updateLabel(FastDistanceData data) {
        if (data instanceof FastDistanceVectorData) {
            FastDistanceVectorData vectorData = (FastDistanceVectorData)data;
            double vectorLabel = Math.sqrt(MatVecOp.dot(vectorData.vector, vectorData.vector));
            if(vectorLabel > 0){
                vectorData.vector.scaleEqual(1.0 / vectorLabel);
            }
        } else {
            FastDistanceMatrixData matrix = (FastDistanceMatrixData)data;
            int vectorSize = matrix.vectors.numRows();
            double[] matrixData = matrix.vectors.getData();

            int cnt = 0;
            while (cnt < matrixData.length) {
                int endIndex = cnt + vectorSize;
                double vectorLabel = 0.;
                while (cnt < endIndex) {
                    vectorLabel += matrixData[cnt] * matrixData[cnt];
                    cnt++;
                }
                vectorLabel = Math.sqrt(vectorLabel);
                if(vectorLabel > 0) {
                    BLAS.scal(1.0 / vectorLabel, matrixData, cnt - vectorSize, vectorSize);
                }
            }
        }
    }

    /**
     * distance = 1 - a * b / ||a|| / ||b|
     *
     * @param left  single vector with label(reciprocal of L2 norm)
     * @param right single vector with label(reciprocal of L2 norm)
     * @return the distance
     */
    @Override
    double calc(FastDistanceVectorData left, FastDistanceVectorData right) {
        return 1.0 - MatVecOp.dot(left.vector, right.vector);
    }

    /**
     * distance = 1 - a * b / ||a|| / ||b|
     *
     * @param leftVector   single vector with label(reciprocal of L2 norm)
     * @param rightVectors vectors with labels(reciprocal of L2 norm array)
     * @param res          the distances between leftVector and all the vectors in rightVectors.
     */
    @Override
    void calc(FastDistanceVectorData leftVector, FastDistanceMatrixData rightVectors, double[] res) {
        Arrays.fill(res, 1.0);
        BLAS.gemv(-1.0, rightVectors.vectors, true, leftVector.vector, 1.0, new DenseVector(res));
    }

    /**
     * distance = 1 - a * b / ||a|| / ||b|
     *
     * @param left  vectors with labels(reciprocal of L2 norm array)
     * @param right vectors with labels(reciprocal of L2 norm array)
     * @param res   the distances between all the vectors in left and all the vectors in right.
     */
    @Override
    void calc(FastDistanceMatrixData left, FastDistanceMatrixData right, DenseMatrix res) {
        Arrays.fill(res.getData(), 1.0);
        BLAS.gemm(-1.0, right.vectors, true, left.vectors, false, 1.0, res);
    }

}
