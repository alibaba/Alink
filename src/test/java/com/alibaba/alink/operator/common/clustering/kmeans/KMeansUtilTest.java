package com.alibaba.alink.operator.common.clustering.kmeans;

import com.alibaba.alink.common.linalg.*;
import com.alibaba.alink.operator.common.clustering.DistanceType;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Unit test for KMeansUtil.
 */
public class KMeansUtilTest {
    private final FastDistance distance = new EuclideanDistance();
    private final int vectorSize = 2;
    private final int length = 10;

    private final List<Row> modelRows = Arrays.asList(
        Row.of(0L, "{\"vectorCol\":\"\\\"Y\\\"\",\"latitudeCol\":null,\"longitudeCol\":null,"
            + "\"distanceType\":\"\\\"EUCLIDEAN\\\"\",\"k\":\"2\",\"vectorSize\":\"3\"}"),
        Row.of(1048576L, "{\"clusterId\":0,\"weight\":3.0,\"vec\":{\"data\":[9.1,9.1,9.1]}}"),
        Row.of(2097152L, "{\"clusterId\":1,\"weight\":3.0,\"vec\":{\"data\":[0.1,0.1,0.1]}}")
    );

    @Test
    public void buildCentroidsMatrixTest() {
        List<FastDistanceVectorData> samples = new ArrayList<>();
        List<Vector> vectorList = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            vectorList.add(DenseVector.ones(vectorSize).scale(i));
        }
        vectorList.forEach(vec -> samples.add(distance.prepareVectorData(Tuple2.of(vec, null))));
        FastDistanceMatrixData matrixData = KMeansUtil.buildCentroidsMatrix(samples, distance, vectorSize);
        DenseMatrix matrix = matrixData.getVectors();
        Assert.assertEquals(matrix.numCols(), length);
        Assert.assertEquals(matrix.numRows(), vectorSize);
        double[] label = matrixData.getLabel().getData();
        for (int i = 0; i < vectorList.size(); i++) {
            Assert.assertEquals(label[i], vectorList.get(i).normL2Square(), 0.001);
        }
    }

    @Test
    public void updateSumMatrixTest() {
        int sampleWeight = 1;
        KMeansPredictModelData predictModelData = new KMeansModelDataConverter().load(modelRows);
        List<FastDistanceVectorData> samples = new ArrayList<>();
        List<Vector> vectorList = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            vectorList.add(DenseVector.ones(predictModelData.params.vectorSize).scale(i));
        }
        vectorList.forEach(vec -> samples
            .add(((FastDistance)predictModelData.params.distanceType.getContinuousDistance()).prepareVectorData(Tuple2.of(vec, null))));

        DenseMatrix distanceMatrix = new DenseMatrix(predictModelData.params.k, 1);
        double[] sumMatrix = new double[predictModelData.params.k * (predictModelData.params.vectorSize + 1)];
        for (FastDistanceVectorData sample : samples) {
            KMeansUtil.updateSumMatrix(
                sample,
                sampleWeight,
                predictModelData.centroids,
                predictModelData.params.vectorSize,
                sumMatrix,
                predictModelData.params.k,
                (FastDistance)predictModelData.params.distanceType.getContinuousDistance(),
                distanceMatrix);
        }

        Arrays.equals(sumMatrix, new double[] {35.0, 35.0, 35.0, 5.0, 10.0, 10.0, 10.0, 5.0});
    }

    @Test
    public void updateSumMatrixSparseTest() {
        int sampleWeight = 1;
        KMeansPredictModelData predictModelData = new KMeansModelDataConverter().load(modelRows);
        int vectorSize = predictModelData.params.vectorSize;
        List<FastDistanceVectorData> samples = new ArrayList<>();
        List<Vector> vectorList = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            vectorList.add(new SparseVector(vectorSize, new int[]{i % vectorSize}, new double[]{i * i}));
        }
        vectorList.forEach(vec -> samples
            .add(((FastDistance)predictModelData.params.distanceType.getContinuousDistance()).prepareVectorData(Tuple2.of(vec, null))));

        DenseMatrix distanceMatrix = new DenseMatrix(predictModelData.params.k, 1);
        double[] sumMatrix = new double[predictModelData.params.k * (vectorSize + 1)];
        for (FastDistanceVectorData sample : samples) {
            KMeansUtil.updateSumMatrix(
                sample,
                sampleWeight,
                predictModelData.centroids,
                predictModelData.params.vectorSize,
                sumMatrix,
                predictModelData.params.k,
                (FastDistance)predictModelData.params.distanceType.getContinuousDistance(),
                distanceMatrix);
        }

        Arrays.equals(sumMatrix, new double[] {117.0, 65.0, 89.0, 6.0, 9.0, 1.0, 4.0, 4.0});
    }


    @Test
    public void transformPredictDataToTrainDataTest() {
        KMeansPredictModelData predictModelData = new KMeansModelDataConverter().load(modelRows);

        Assert.assertEquals(predictModelData.params.k, 2);
        Assert.assertEquals(predictModelData.params.vectorSize, 3);
        Assert.assertEquals(predictModelData.params.vectorColName, "Y");
        Assert.assertNull(predictModelData.params.longtitudeColName);
        Assert.assertNull(predictModelData.params.latitudeColName);
        Assert.assertEquals(predictModelData.params.distanceType, DistanceType.EUCLIDEAN);

        Assert.assertEquals(predictModelData.centroids.getVectors(), new DenseMatrix(3, 2, new double[]{9.1, 9.1, 9.1, 0.1, 0.1, 0.1}));
    }

    @Test
    public void getClusterFastDistanceTest() {
        KMeansPredictModelData predictModelData = new KMeansModelDataConverter().load(modelRows);
        FastDistanceVectorData sample = distance.prepareVectorData(Tuple2.of(VectorUtil.getVector("9 9 9"), null));

        DenseMatrix distanceMatrix = new DenseMatrix(predictModelData.params.k, 1);
        Tuple2<Integer, Double> tuple2 = KMeansUtil.getClosestClusterIndex(sample,
            predictModelData.centroids,
            predictModelData.params.k,
            (FastDistance)predictModelData.params.distanceType.getContinuousDistance(),
            distanceMatrix);

        Assert.assertEquals(tuple2.f0.intValue(), 0);
        Assert.assertEquals(tuple2.f1, 0.173, 0.01);
        double[] distanceMatrixData = distanceMatrix.getData();
        Assert.assertEquals(distanceMatrixData[0], 0.173, 0.01);
        Assert.assertEquals(distanceMatrixData[1], 15.415, 0.01);
    }

    @Test
    public void getClusterContinuousDistanceTest() {
        Vector sample = VectorUtil.getVector("9 9 9");

        KMeansPredictModelData predictModelData = new KMeansModelDataConverter().load(modelRows);
        KMeansTrainModelData modelData = KMeansUtil.transformPredictDataToTrainData(predictModelData);
        Tuple2<Integer, Double> tuple2 = KMeansUtil.getClosestClusterIndex(modelData, sample,
            (FastDistance)modelData.params.distanceType.getContinuousDistance());

        Assert.assertEquals(tuple2.f0.intValue(), 0);
        Assert.assertEquals(tuple2.f1, 0.173, 0.01);
    }

    @Test
    public void getKmeansPredictColIdxsTest() {
        KMeansTrainModelData.ParamSummary params = new KMeansTrainModelData.ParamSummary();
        params.vectorColName = "vec";
        params.latitudeColName = null;
        params.longtitudeColName = null;
        params.k = 2;
        params.vectorSize = vectorSize;
        params.distanceType = DistanceType.EUCLIDEAN;

        String[] colNames = new String[] {"id", "vec", "lat", "lon"};
        Assert.assertArrayEquals(KMeansUtil.getKmeansPredictColIdxs(params, colNames), new int[] {1});

        params.vectorColName = null;
        params.latitudeColName = "lat";
        params.longtitudeColName = "lon";
        params.distanceType = DistanceType.HAVERSINE;
        Assert.assertArrayEquals(KMeansUtil.getKmeansPredictColIdxs(params, colNames), new int[] {2, 3});
    }

    @Test
    public void getKMeansPredictVectorTest() {
        int[] colIdxs = new int[] {1};
        Row row = Row.of(1, "0 0 0");
        Assert.assertEquals(KMeansUtil.getKMeansPredictVector(colIdxs, row), DenseVector.zeros(3));
    }
}