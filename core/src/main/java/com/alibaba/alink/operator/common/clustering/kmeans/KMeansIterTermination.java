package com.alibaba.alink.operator.common.clustering.kmeans;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompareCriterionFunction;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculate the distance between the pre-round centers and current centers. Judge whether algorithm converges.
 */
public class KMeansIterTermination extends CompareCriterionFunction {
    private static final Logger LOG = LoggerFactory.getLogger(KMeansIterTermination.class);
    private FastDistance distance;
    private double tol;
    private transient DenseMatrix distanceMatrix;

    public KMeansIterTermination(FastDistance distance, double tol) {
        this.distance = distance;
        this.tol = tol;
    }

    @Override
    public boolean calc(ComContext context) {
        Integer k = context.getObj(KMeansTrainBatchOp.K);
        Tuple2<Integer, FastDistanceMatrixData> centroids1 = context.getObj(KMeansTrainBatchOp.CENTROID1);
        Tuple2<Integer, FastDistanceMatrixData> centroids2 = context.getObj(KMeansTrainBatchOp.CENTROID2);

        distanceMatrix = distance.calc(centroids1.f1, centroids2.f1, distanceMatrix);

        for (int id = 0; id < k; id++) {
            double d = distanceMatrix.get(id, id);
            LOG.info("StepNo {}, TaskId {} ||centroid-prev_centroid|| {}",
                context.getStepNo(), context.getTaskId(), d);
            if (d >= tol) {
                return false;
            }
        }
        return true;
    }
}
