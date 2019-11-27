package com.alibaba.alink.operator.common.clustering.kmeans;

import com.alibaba.alink.common.linalg.*;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.clustering.DistanceType;
import com.alibaba.alink.operator.common.distance.ContinuousDistance;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Common functions for KMeans.
 */
public class KMeansUtil implements Serializable {
    /**
     * Build the FastDistanceMatrixData from a list of FastDistanceVectorData.
     *
     * @param vectors    list of FastDistanceVectorData.
     * @param distance   FastDistance.
     * @param vectorSize vectorSize.
     * @return FastDistanceMatrixData
     */
    public static FastDistanceMatrixData buildCentroidsMatrix(List<FastDistanceVectorData> vectors,
                                                              FastDistance distance,
                                                              int vectorSize) {
        DenseMatrix matrix = new DenseMatrix(vectorSize, vectors.size());
        for (int i = 0; i < vectors.size(); i++) {
            MatVecOp.appendVectorToMatrix(matrix, false, i, vectors.get(i).getVector());
        }
        FastDistanceMatrixData centroid = new FastDistanceMatrixData(matrix);
        distance.updateLabel(centroid);
        return centroid;
    }

    /**
     * Find the closest centroid from centroids for sample, and add the sample to sumMatrix.
     *
     * @param sample         query sample.
     * @param sampleWeight   sample weight.
     * @param centroids      centroids.
     * @param vectorSize     vectorsize.
     * @param sumMatrix      the sumMatrix to be update.
     * @param k              centroid number.
     * @param fastDistance   distance.
     * @param distanceMatrix preallocated distance result matrix.
     * @return the closest cluster index.
     */
    public static int updateSumMatrix(FastDistanceVectorData sample,
                                      long sampleWeight,
                                      FastDistanceMatrixData centroids,
                                      int vectorSize,
                                      double[] sumMatrix,
                                      int k,
                                      FastDistance fastDistance,
                                      DenseMatrix distanceMatrix) {
        Preconditions.checkNotNull(sumMatrix);
        Preconditions.checkNotNull(distanceMatrix);
        Preconditions.checkArgument(distanceMatrix.numRows() == centroids.getVectors().numCols() &&
            distanceMatrix.numCols() == 1, "Memory not preallocated!");

        fastDistance.calc(sample, centroids, distanceMatrix);
        int clusterIndex = getClosestClusterIndex(sample, centroids, k, fastDistance, distanceMatrix).f0;
        int startIndex = clusterIndex * (vectorSize + 1);
        Vector vec = sample.getVector();
        if (vec instanceof DenseVector) {
            BLAS.axpy(vectorSize, sampleWeight, ((DenseVector)vec).getData(), 0, sumMatrix, startIndex);
        } else {
            SparseVector sparseVector = (SparseVector)vec;
            sparseVector.forEach((index, value) -> sumMatrix[startIndex + index] += sampleWeight * value);
        }
        sumMatrix[startIndex + vectorSize] += sampleWeight;
        return clusterIndex;
    }

    /**
     * Find the closest cluster index.
     *
     * @param sample         query sample.
     * @param centroids      centroids.
     * @param k              cluster number.
     * @param distance       FastDistance.
     * @param distanceMatrix Preallocated distance matrix.
     * @return the closest cluster index and distance.
     */
    public static Tuple2<Integer, Double> getClosestClusterIndex(FastDistanceVectorData sample,
                                                                 FastDistanceMatrixData centroids,
                                                                 int k,
                                                                 FastDistance distance,
                                                                 DenseMatrix distanceMatrix) {
        getClusterDistances(sample, centroids, distance, distanceMatrix);
        double[] data = distanceMatrix.getData();
        int index = getMinPointIndex(data, k);
        return Tuple2.of(index, data[index]);
    }

    /**
     * Find the distances from the centroids.
     * @param sample         query sample.
     * @param centroids      centroids.
     * @param distance       FastDistance.
     * @param distanceMatrix Preallocated distance matrix.
     * @return the distance array.
     */
    public static double[] getClusterDistances(FastDistanceVectorData sample,
                                               FastDistanceMatrixData centroids,
                                               FastDistance distance,
                                               DenseMatrix distanceMatrix) {
        Preconditions.checkNotNull(distanceMatrix);
        Preconditions.checkArgument(distanceMatrix.numRows() == centroids.getVectors().numCols() &&
            distanceMatrix.numCols() == 1, "Memory not preallocated!");

        distance.calc(sample, centroids, distanceMatrix);
        return distanceMatrix.getData();
    }

    /**
     * Find the closest cluster index.
     *
     * @param trainModelData trainModel
     * @param sample         query sample
     * @param distance       ContinuousDistance
     * @return the index and distance.
     */
    public static Tuple2<Integer, Double> getClosestClusterIndex(KMeansTrainModelData trainModelData,
                                                                 Vector sample,
                                                                 ContinuousDistance distance) {
        double[] distances = getClusterDistances(trainModelData, sample, distance);
        int index = getMinPointIndex(distances, trainModelData.params.k);
        return Tuple2.of(index, distances[index]);
    }

    /**
     * Find the distances from the centroids.
     *
     * @param trainModelData trainModel
     * @param sample         query sample
     * @param distance       ContinuousDistance
     * @return the distance array.
     */
    public static double[] getClusterDistances(KMeansTrainModelData trainModelData,
                                               Vector sample,
                                               ContinuousDistance distance) {
        double[] res = new double[trainModelData.params.k];
        for(int i = 0; i < res.length; i++){
            res[i] = distance.calc(trainModelData.getClusterVector(i), sample);
        }
        return res;
    }

    public static int getMinPointIndex(double[] data, int endIndex){
        Preconditions.checkArgument(endIndex <= data.length, "End index must be less than data length!");
        int index = -1;
        double min = Double.MAX_VALUE;
        for (int i = 0; i < endIndex; i++) {
            if (data[i] < min) {
                index = i;
                min = data[i];
            }
        }
        return index;
    }

    /**
     * Get the selected columns indexes from the input columns. Support vector input or latitudeCol and longtitude
     * inputs.
     *
     * @param params   ParamSummary.
     * @param dataCols input columns.
     * @return selected columns indexes.
     */
    public static int[] getKmeansPredictColIdxs(KMeansTrainModelData.ParamSummary params, String[] dataCols) {
        Preconditions.checkArgument((null == params.longtitudeColName) == (null == params.latitudeColName),
            "Model Format error!");
        Preconditions.checkArgument(params.distanceType.equals(DistanceType.HAVERSINE) == (null == params.vectorColName
                && null != params.longtitudeColName),
            "Model Format error!");
        int[] colIdxs;
        if (null != params.vectorColName) {
            colIdxs = new int[1];
            colIdxs[0] = TableUtil.findColIndex(dataCols, params.vectorColName);
            Preconditions.checkArgument(colIdxs[0] >= 0, "can't find vector column in predict data!");
        } else {
            colIdxs = new int[2];
            colIdxs[0] = TableUtil.findColIndex(dataCols, params.latitudeColName);
            colIdxs[1] = TableUtil.findColIndex(dataCols, params.longtitudeColName);
            Preconditions.checkArgument(colIdxs[0] >= 0 && colIdxs[1] >= 0,
                "can't find latitude or longtitude column in predict data!");
        }
        return colIdxs;
    }

    /**
     * Extract the vector from Row.
     *
     * @param colIdxs selected column indices.
     * @param row     Row.
     * @return the vector.
     */
    public static Vector getKMeansPredictVector(int[] colIdxs, Row row) {
        Vector vec;
        if (colIdxs.length > 1) {
            vec = new DenseVector(2);
            vec.set(0, ((Number)row.getField(colIdxs[0])).doubleValue());
            vec.set(1, ((Number)row.getField(colIdxs[1])).doubleValue());
        } else {
            vec = VectorUtil.getVector(row.getField(colIdxs[0]));
        }
        return vec;
    }

    /**
     * Transform KMeansPredictModelData to KMeansTrainModelData.
     *
     * @param predictModelData KMeansPredictModelData.
     * @return KMeansTrainModelData.
     */
    public static KMeansTrainModelData transformPredictDataToTrainData(KMeansPredictModelData predictModelData) {
        KMeansTrainModelData modelData = new KMeansTrainModelData();
        modelData.params = predictModelData.params;
        modelData.centroids = new ArrayList<>();
        for (int i = 0; i < predictModelData.params.k; i++) {
            KMeansTrainModelData.ClusterSummary clusterSummary = new KMeansTrainModelData.ClusterSummary(
                predictModelData.getClusterVector(i),
                predictModelData.getClusterId(i),
                predictModelData.getClusterWeight(i));
            modelData.centroids.add(clusterSummary);
        }
        return modelData;
    }

    /**
     * Transform KMeansTrainModelData to KMeansPredictModelData.
     *
     * @param trainModelData KMeansTrainModelData.
     * @return KMeansPredictModelData.
     */
    public static KMeansPredictModelData transformTrainDataToPredictData(KMeansTrainModelData trainModelData) {
        KMeansPredictModelData modelData = new KMeansPredictModelData();
        modelData.params = trainModelData.params;
        DenseMatrix denseMatrix = new DenseMatrix(trainModelData.params.vectorSize, trainModelData.params.k);
        Row[] rows = new Row[trainModelData.params.k];
        int index = 0;
        for (int i = 0; i < trainModelData.centroids.size(); i++) {
            MatVecOp.appendVectorToMatrix(denseMatrix, false, index, trainModelData.getClusterVector(i));
            rows[index] = Row.of(trainModelData.getClusterId(i), trainModelData.getClusterWeight(i));
            index++;
        }
        modelData.centroids = new FastDistanceMatrixData(denseMatrix, rows);
        ((FastDistance)modelData.params.distanceType.getContinuousDistance()).updateLabel(modelData.centroids);
        return modelData;
    }

    public static double[] getProbArrayFromDistanceArray(double[] distances){
        double sum = StatUtils.sum(distances);
        double ratio = 1.0 / sum / (distances.length - 1);
        double[] probs = new double[distances.length];
        Arrays.fill(probs, 1.0 / (distances.length - 1));
        BLAS.axpy(-ratio, distances, probs);
        return probs;
    }

    /**
     * Load KMeansTrainModelData from saved model.
     *
     * @param params saved params.
     * @param data   saved data.
     * @return KMeansTrainModelData.
     */
    public static KMeansTrainModelData loadModelForTrain(Params params, Iterable<String> data) {
        KMeansTrainModelData trainModelData = new KMeansTrainModelData();
        trainModelData.params = new KMeansTrainModelData.ParamSummary(params);
        trainModelData.centroids = new ArrayList<>(trainModelData.params.k);
        data.forEach(s -> {
            try {
                trainModelData.centroids.add(JsonConverter.fromJson(s, KMeansTrainModelData.ClusterSummary.class));
            } catch (Exception e) {
                OldClusterSummary oldClusterSummary = JsonConverter.fromJson(s, OldClusterSummary.class);
                DenseVector vec;
                if (oldClusterSummary.center.contains("data")) {
                    vec = JsonConverter.fromJson(oldClusterSummary.center, DenseVector.class);
                } else {
                    vec = new DenseVector(JsonConverter.fromJson(oldClusterSummary.center, double[].class));
                }
                KMeansTrainModelData.ClusterSummary clusterSummary = new KMeansTrainModelData.ClusterSummary(
                    vec,
                    oldClusterSummary.clusterId,
                    oldClusterSummary.weight
                );
                trainModelData.centroids.add(clusterSummary);
            }
        });

        return trainModelData;
    }

    static class OldClusterSummary implements Serializable {
        public long clusterId;
        public double weight;
        public String center;
        public DenseVector vec;
    }
}
