package com.alibaba.alink.operator.common.clustering.kmeans;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Allocate memory for pre-round centers and current centers.
 */
public class KMeansPreallocateCentroid extends ComputeFunction {
    private static final Logger LOG = LoggerFactory.getLogger(KMeansPreallocateCentroid.class);

    @Override
    public void calc(ComContext context) {
        if (context.getStepNo() == 1) {

            List<FastDistanceMatrixData> initCentroids = context.getObj(KMeansTrainBatchOp.INIT_CENTROID);
            List<Integer> list = context.getObj(KMeansTrainBatchOp.KMEANS_STATISTICS);
            Integer vectorSize = list.get(0);
            context.putObj(KMeansTrainBatchOp.VECTOR_SIZE, vectorSize);

            FastDistanceMatrixData centroid = initCentroids.get(0);
            Preconditions.checkArgument(centroid.getVectors().numRows() == vectorSize,
                "Init centroid error, size not equal!");
            LOG.info("Init centroids, initial centroid size {}", centroid.getVectors().numCols());
            context.putObj(KMeansTrainBatchOp.CENTROID1, Tuple2.of(context.getStepNo() - 1, centroid));
            context.putObj(KMeansTrainBatchOp.CENTROID2,
                Tuple2.of(context.getStepNo() - 1, new FastDistanceMatrixData(centroid)));
            context.putObj(KMeansTrainBatchOp.K, centroid.getVectors().numCols());
        }
    }
}
