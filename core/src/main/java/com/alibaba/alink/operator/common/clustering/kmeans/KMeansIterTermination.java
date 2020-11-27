package com.alibaba.alink.operator.common.clustering.kmeans;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompareCriterionFunction;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculate the distance between the pre-round centers and current centers. Judge whether algorithm converges.
 */
public class KMeansIterTermination extends CompareCriterionFunction {
	private static final Logger LOG = LoggerFactory.getLogger(KMeansIterTermination.class);
	private static int MAX_K_NUMBER = 10000;

	private static final long serialVersionUID = 6636978614737723605L;
	private FastDistance distance;
	private double tol;
	private transient DenseMatrix distanceMatrix;
	private transient double[] centroid1;
	private transient double[] centroid2;

	public KMeansIterTermination(FastDistance distance, double tol) {
		this.distance = distance;
		this.tol = tol;
	}

	@Override
	public boolean calc(ComContext context) {
		Integer k = context.getObj(KMeansTrainBatchOp.K);
		Integer vectorSize = context.getObj(KMeansTrainBatchOp.VECTOR_SIZE);
		Tuple2 <Integer, FastDistanceMatrixData> centroids1 = context.getObj(KMeansTrainBatchOp.CENTROID1);
		Tuple2 <Integer, FastDistanceMatrixData> centroids2 = context.getObj(KMeansTrainBatchOp.CENTROID2);

		if (k <= MAX_K_NUMBER) {
			distanceMatrix = distance.calc(centroids1.f1, centroids2.f1, distanceMatrix);

			for (int id = 0; id < k; id++) {
				double d = distanceMatrix.get(id, id);
				LOG.info("StepNo {}, TaskId {} ||centroid-prev_centroid|| {}",
					context.getStepNo(), context.getTaskId(), d);
				if (d >= tol) {
					return false;
				}
			}
		} else {
			double[] data1 = centroids1.f1.getVectors().getData();
			double[] data2 = centroids2.f1.getVectors().getData();
			if (null == centroid1) {
				centroid1 = new double[vectorSize];
				centroid2 = new double[vectorSize];
			}
			for (int id = 0; id < k; id++) {
				System.arraycopy(data1, id * vectorSize, centroid1, 0, vectorSize);
				System.arraycopy(data2, id * vectorSize, centroid2, 0, vectorSize);
				double d = distance.calc(centroid1, centroid2);
				LOG.info("StepNo {}, TaskId {} ||centroid-prev_centroid|| {}",
					context.getStepNo(), context.getTaskId(), d);
				if (d >= tol) {
					return false;
				}
			}
		}

		return true;
	}
}
