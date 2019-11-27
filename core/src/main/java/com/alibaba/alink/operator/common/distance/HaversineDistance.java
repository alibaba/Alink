package com.alibaba.alink.operator.common.distance;

import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import org.apache.flink.util.Preconditions;

/**
 * Haversine formula: find distance between two points on a sphere.
 * <p>
 * https://en.wikipedia.org/wiki/Haversine_formula.
 * <p>
 * haversine(theta) = sin(theta / 2)^2
 * <p>
 * h = haversine(lat1 - lat2) + Math.cos(lat1) * Math.cos(lat2) * haverSine(lon1 - lon2)
 * <p>
 * d = 2 * EARTH_RADIUS * arcsin(sqrt(h))
 * <p>
 * Latitude and longitude are in degree format. If input is a vector, it's required the first value is latitude and the
 * second value is longitude.
 */
public class HaversineDistance extends FastDistance{
    /**
     * Earth radius.
     */
    private static int EARTH_RADIUS = 6371;

    /**
     * Degree to radian costant.
     */
    private static double DEGREE_TO_RADIAN_CONSTANT = Math.PI / 180;

    /**
     * Label size.
     */
    private static int LABEL_SIZE = 3;

    /**
     * Vector size;
     */
    private static int VECTOR_SIZE = 2;

    /**
     * haversine(theta) = sin(theta / 2)^2 = (1 - cos(theta)) / 2
     *
     * @param theta degree
     * @return haversine
     */
    static double haverSine(double theta) {
        return (1 - Math.cos(theta)) / 2;
    }

    /**
     * Transform degree to radia.
     *
     * @param data degree.
     * @return radian.
     */
    static double degreeToRadian(double data) {
        return data * DEGREE_TO_RADIAN_CONSTANT;
    }

    private static double cal(double h) {
        return 2 * EARTH_RADIUS * Math.asin(Math.min(1.0, Math.sqrt(Math.abs(h))));
    }

    /**
     * sin(lat), cos(lat)cos(lon), sin(lat)sin(lon)
     */
    private static double[] vectorLabel(double latitude, double longitude) {
        double lat = degreeToRadian(latitude);
        double lon = degreeToRadian(longitude);

        double latSin = Math.sin(lat);
        double latCos = Math.cos(lat);
        double lonSin = Math.sin(lon);
        double lonCos = Math.cos(lon);

        return new double[] {latSin, latCos * lonCos, latCos * lonSin};
    }

    /**
     * The shortest distance between two points on the surface of the Earth
     *
     * @param latitude1  latitude of the first point.
     * @param longitude1 longitude of the first point.
     * @param latitude2  latitude of the second point.
     * @param longitude2 longitude of the second point.
     * @return distance.
     */
    public double calc(double latitude1, double longitude1, double latitude2, double longitude2) {
        double lat1 = degreeToRadian(latitude1);
        double lon1 = degreeToRadian(longitude1);
        double lat2 = degreeToRadian(latitude2);
        double lon2 = degreeToRadian(longitude2);

        double vLat = lat2 - lat1;
        double vLon = lon2 - lon1;
        double h = haverSine(vLat) + Math.cos(lat1) * Math.cos(lat2) * haverSine(vLon);
        return cal(h);
    }

    /**
     * The shortest distance between two points on the surface of the Earth
     *
     * @param array1 [latitude, longitude] of the first point.
     * @param array2 [latitude, longitude] of the second point.
     * @return distance.
     */
    @Override
    public double calc(double[] array1, double[] array2) {
        Preconditions.checkState(array1.length == VECTOR_SIZE && array2.length == VECTOR_SIZE,
            "HaversineDistance only supports vector size 2, the first value is latitude and the second value is "
                + "longitude");

        return calc(array1[0], array1[1], array2[0], array2[1]);
    }

    /**
     * The shortest distance between two points on the surface of the Earth
     *
     * @param vec1 [latitude, longitude] of the first point.
     * @param vec2 [latitude, longitude] of the second point.
     * @return distance.
     */
    @Override
    public double calc(Vector vec1, Vector vec2) {
        Preconditions.checkState(vec1.size() == VECTOR_SIZE && vec2.size() == VECTOR_SIZE,
            "HaversineDistance only supports vector size 2, the first value is latitude and the second value is "
                + "longitude");

        return calc(vec1.get(0), vec1.get(1), vec2.get(0), vec2.get(1));
    }

    /**
     * For Haversine distance, d = 2 * EARTH_RADIUS * arcsin(sqrt(h)),
     * <p>
     * h = haversine(lat1 - lat2) + Math.cos(lat1) * Math.cos(lat2) * haverSine(lon1 - lon2)
     * <p>
     * h = 0.5 * (1 - sin(lat1)*sin(lat2) - cos(lat1)cos(lon1)*cos(lat2)cos(lon2) -
     * cos(lat1)sin(lon1)cos(lat2)sin(lon2)
     * <p>
     * h = 0.5 * (1 - [sin(lat1), cos(lat1)cos(lon1), cos(lat1)sin(lon1)] * [sin(lat2), cos(lat2)cos(lon2),
     * cos(lat2)sin(lon2)]
     * <p>
     * d = 2 * EARTH_RADIUS * arcsin(sqrt(h))
     * <p>
     * So we can pre-calculate the sin(latitude), cos(latitude) * cos(longitude),cos(latitude) * sin(longitude) of the
     * point, and when we need to calculate the distance with another point, only dot product is calculated.
     * <p>
     * For FastDistanceVectorData, the label is a three-dimension vector. For FastDistanceMatrixData, the label is a 3 X
     * n DenseMatrix, n is the number of vectors saved in the matrix.
     *
     * @param data FastDistanceData.
     */
    @Override
    public void updateLabel(FastDistanceData data) {
        if (data instanceof FastDistanceVectorData) {
            FastDistanceVectorData vectorData = (FastDistanceVectorData)data;
            Vector vec = vectorData.getVector();
            Preconditions.checkState(vec.size() == VECTOR_SIZE,
                "HaversineDistance only supports vector size 2, the first value is latitude and the second value is "
                    + "longitude");
            if (vectorData.label == null || vectorData.label.size() != LABEL_SIZE) {
                vectorData.label = new DenseVector(LABEL_SIZE);
            }
            vectorData.label = new DenseVector(vectorLabel(vec.get(0), vec.get(1)));
        } else {
            FastDistanceMatrixData matrix = (FastDistanceMatrixData)data;
            if (matrix.label == null || matrix.label.numRows() != LABEL_SIZE || matrix.label.numCols() != matrix.vectors
                .numCols()) {
                matrix.label = new DenseMatrix(LABEL_SIZE, matrix.vectors.numCols());
            }
            double[] matrixData = matrix.vectors.getData();
            Preconditions.checkState(matrixData.length % VECTOR_SIZE == 0,
                "HaversineDistance only supports vector size 2, the first value is latitude and the second value is "
                    + "longitude");

            double[] normData = matrix.label.getData();
            int labelCnt = 0;
            for (int i = 0; i < matrixData.length; i += VECTOR_SIZE) {
                double[] norm = vectorLabel(matrixData[i], matrixData[i + 1]);
                normData[labelCnt++] = norm[0];
                normData[labelCnt++] = norm[1];
                normData[labelCnt++] = norm[2];
            }
        }
    }

    /**
     * h = 0.5 * (1 - [sin(lat1), cos(lat1)cos(lon1), cos(lat1)sin(lon1)] * [sin(lat2), cos(lat2)cos(lon2),
     * cos(lat2)sin(lon2)]
     * <p>
     * d = 2 * EARTH_RADIUS * arcsin(sqrt(h))
     *
     * @param left  single vector with label(sin(lat), cos(lat)cos(lon), cos(lat)sin(lon))
     * @param right single vector with label(sin(lat), cos(lat)cos(lon), cos(lat)sin(lon))
     * @return the distance
     */
    @Override
    double calc(FastDistanceVectorData left, FastDistanceVectorData right) {
        return cal(0.5 * (1 - BLAS.dot(left.label, right.label)));

    }

    /**
     * h = 0.5 * (1 - [sin(lat1), cos(lat1)cos(lon1), cos(lat1)sin(lon1)] * [sin(lat2), cos(lat2)cos(lon2),
     * cos(lat2)sin(lon2)]
     * <p>
     * d = 2 * EARTH_RADIUS * arcsin(sqrt(h))
     *
     * @param leftVector   single vector with label
     * @param rightVectors vectors with labels
     * @param res          the distances between leftVector and all the vectors in rightVectors.
     */
    @Override
    void calc(FastDistanceVectorData leftVector, FastDistanceMatrixData rightVectors, double[] res) {
        BLAS.gemv(-0.5, rightVectors.label, true, leftVector.label, 0.0, new DenseVector(res));
        for (int i = 0; i < res.length; i++) {
            res[i] = cal(0.5 + res[i]);
        }
    }

    /**
     * h = 0.5 * (1 - [sin(lat1), cos(lat1)cos(lon1), cos(lat1)sin(lon1)] * [sin(lat2), cos(lat2)cos(lon2),
     * cos(lat2)sin(lon2)]
     * <p>
     * d = 2 * EARTH_RADIUS * arcsin(sqrt(h))
     *
     * @param left  vectors with labels
     * @param right vectors with labels
     * @param res   the distances between all the vectors in left and all the vectors in right.
     */
    @Override
    void calc(FastDistanceMatrixData left, FastDistanceMatrixData right, DenseMatrix res) {
        BLAS.gemm(-0.5, right.label, true, left.label, false, 0.0, res);
        double[] data = res.getData();
        for (int i = 0; i < data.length; i++) {
            data[i] = cal(0.5 + data[i]);
        }
    }
}
