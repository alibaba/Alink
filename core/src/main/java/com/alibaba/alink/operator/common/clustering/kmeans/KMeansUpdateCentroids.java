package com.alibaba.alink.operator.common.clustering.kmeans;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Update the centroids based on the sum of points and point number belonging to the same cluster.
 */
public class KMeansUpdateCentroids extends ComputeFunction {
    private static final Logger LOG = LoggerFactory.getLogger(KMeansUpdateCentroids.class);
    private FastDistance distance;

    public KMeansUpdateCentroids(FastDistance distance) {
        this.distance = distance;
    }

    @Override
    public void calc(ComContext context) {
        LOG.info("StepNo {}, TaskId {} Update cluster begins!", context.getStepNo(),
            context.getTaskId());

        Integer vectorSize = context.getObj(KMeansTrainBatchOp.VECTOR_SIZE);
        Integer k = context.getObj(KMeansTrainBatchOp.K);

        double[] sumMatrixData = context.getObj(KMeansTrainBatchOp.CENTROID_ALL_REDUCE);

        Tuple2<Integer, FastDistanceMatrixData> stepNumCentroids;
        if (context.getStepNo() % 2 == 0) {
            stepNumCentroids = context.getObj(KMeansTrainBatchOp.CENTROID2);
        } else {
            stepNumCentroids = context.getObj(KMeansTrainBatchOp.CENTROID1);
        }

        stepNumCentroids.f0 = context.getStepNo();

        context.putObj(KMeansTrainBatchOp.K,
            updateCentroids(stepNumCentroids.f1, k, vectorSize, sumMatrixData, distance));
        LOG.info("StepNo {}, TaskId {} Update cluster ends!", context.getStepNo(),
            context.getTaskId());
    }

    static int updateCentroids(FastDistanceMatrixData matrixData, int k, int vectorSize, double[] buffer,
                               FastDistance distance) {
        int index = 0;
        DenseMatrix matrix = matrixData.getVectors();
        double[] data = matrix.getData();
        Arrays.fill(data, 0.0);
        for (int clusterId = 0; clusterId < k; clusterId++) {
            int startIndex = clusterId * (vectorSize + 1);
            double weight = buffer[startIndex + vectorSize];
            if (weight == 0) {
                continue;
            }
            BLAS.axpy(vectorSize, 1.0 / weight, buffer, startIndex, data, index * vectorSize);
            index++;
        }
        distance.updateLabel(matrixData);
        return index;
    }
}
