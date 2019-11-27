package com.alibaba.alink.operator.common.clustering.kmeans;

import com.alibaba.alink.common.linalg.*;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Run kmeans locally, it's only used in the initialization of KMeans.
 */
class LocalKmeansFunc implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(LocalKmeansFunc.class);

    /**
     * Max iteration number when running kmeans locally.
     */
    private static int LOCAL_MAX_ITER = 30;

    /**
     * Run K-means++ on the weighted samples. First do the K-means++ initialization and then runs Lloyd's algorithm.
     *
     * @param k          cluster number.
     * @param samples    initial weighted samples.
     * @param distance   distance measure.
     * @param vectorSize the size of vectors.
     * @return the result of kmeans.
     */
    static FastDistanceMatrixData kmeans(int k, long[] sampleWeights, FastDistanceVectorData[] samples,
                                         FastDistance distance, int vectorSize) {

        Random random = new Random(0);

        List<FastDistanceVectorData> initCentroidsList = sampleInitialCentroids(k, sampleWeights, samples, distance,
            random);
        FastDistanceMatrixData initCentroids = KMeansUtil.buildCentroidsMatrix(initCentroidsList, distance, vectorSize);

        boolean converge = false;
        int iteration = 0;
        DenseMatrix sumMatrix = new DenseMatrix(vectorSize + 1, k);
        DenseMatrix distanceMatrix = new DenseMatrix(k, 1);
        double[] sumMatrixData = sumMatrix.getData();
        double[] initCentroidsData = initCentroids.getVectors().getData();
        int[] indices = new int[samples.length];

        while (!converge && iteration < LOCAL_MAX_ITER) {
            iteration++;
            converge = true;
            for (int i = 0; i < samples.length; i++) {
                int clusterId = KMeansUtil.updateSumMatrix(samples[i], sampleWeights[i], initCentroids, vectorSize,
                    sumMatrixData, k, distance, distanceMatrix);
                if (clusterId != indices[i]) {
                    indices[i] = clusterId;
                    converge = false;
                }
            }
            Arrays.fill(initCentroidsData, 0.0);
            for (int i = 0; i < k; i++) {
                int initCentroidsStartIndex = i * vectorSize;
                int sumMatrixStartIndex = initCentroidsStartIndex + i;
                double weight = sumMatrixData[sumMatrixStartIndex + vectorSize];

                if (weight > 0) {
                    BLAS.axpy(vectorSize, 1.0 / weight, sumMatrixData, sumMatrixStartIndex, initCentroidsData,
                        initCentroidsStartIndex);
                } else {
                    int index = random.nextInt(samples.length);
                    MatVecOp.appendVectorToMatrix(initCentroids.getVectors(), false, i, samples[index].getVector());
                }
                distance.updateLabel(initCentroids);
            }
        }
        if (iteration != LOCAL_MAX_ITER) {
            LOG.info("Local kmeans converge with {} steps.", iteration);
        } else {
            LOG.info("Local kmeans reach max iteration number!");
        }
        return initCentroids;
    }

    private static List<FastDistanceVectorData> sampleInitialCentroids(int k,
                                                                       long[] sampleWeights,
                                                                       FastDistanceVectorData[] samples,
                                                                       FastDistance distance,
                                                                       Random random) {
        List<FastDistanceVectorData> initCentroids = new ArrayList<>(k);
        double[] costs = new double[samples.length];
        Arrays.fill(costs, 1.0);
        int index = 0;
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < samples.length && i > 0; j++) {
                double d = distance.calc(samples[j], samples[index]).get(0, 0);
                costs[j] = i > 1 ? Math.min(d, costs[j]) : d;
            }
            index = pickWeight(sampleWeights, costs, random);
            initCentroids.add(samples[index]);
        }
        return initCentroids;
    }

    private static int pickWeight(long[] centroidWeights, double[] costs, Random random) {
        int res = costs.length;
        double[] sum = new double[centroidWeights.length + 1];
        for (int i = 1; i < sum.length; i++) {
            sum[i] = sum[i - 1] + centroidWeights[i - 1] * costs[i - 1];
        }
        double r = random.nextDouble() * sum[sum.length - 1];
        for (int i = 0; i < sum.length; i++) {
            if (sum[i] >= r) {
                res = i;
                break;
            }
        }
        return res == 0 ? 0 : res - 1;
    }
}
