package com.alibaba.alink.operator.common.clustering.kmeans;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp;
import com.alibaba.alink.operator.common.clustering.DistanceType;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * Tranform the centroids to KmeansModel.
 */
public class KMeansOutputModel extends CompleteResultFunction {
	private DistanceType distanceType;
	private String vectorColName;
	private String latitudeColName;
	private String longtitudeColName;

	public KMeansOutputModel(DistanceType distanceType, String vectorColName,
							 final String latitudeColName, final String longitudeColName) {
		this.distanceType = distanceType;
		this.vectorColName = vectorColName;
		this.latitudeColName = latitudeColName;
		this.longtitudeColName = longitudeColName;
	}

	@Override
	public List <Row> calc(ComContext context) {
		if (context.getTaskId() != 0) {
			return null;
		}

		Integer vectorSize = context.getObj(KMeansTrainBatchOp.VECTOR_SIZE);
		Integer k = context.getObj(KMeansTrainBatchOp.K);

		Tuple2<Integer, FastDistanceMatrixData> stepNoCentroids1 = context.getObj(KMeansTrainBatchOp.CENTROID1);
		Tuple2<Integer, FastDistanceMatrixData> stepNoCentroids2 = context.getObj(KMeansTrainBatchOp.CENTROID2);
		double[] buffer = context.getObj(KMeansTrainBatchOp.CENTROID_ALL_REDUCE);

		FastDistanceMatrixData centroid;
		if (stepNoCentroids1.f0 > stepNoCentroids2.f0) {
			centroid = stepNoCentroids1.f1;
		} else {
			centroid = stepNoCentroids2.f1;
		}

		KMeansTrainModelData modelData = new KMeansTrainModelData();
		modelData.centroids = new ArrayList<>();
		DenseMatrix matrix = centroid.getVectors();

		int weightIndex = vectorSize;
		for (int id = 0; id < k; id++) {
			modelData.centroids.add(
				new KMeansTrainModelData.ClusterSummary(
				new DenseVector(matrix.getColumn(id)),
				id,
				buffer[weightIndex]));
			weightIndex += vectorSize + 1;
		}
		modelData.params = new KMeansTrainModelData.ParamSummary();
		modelData.params.k = k;
		modelData.params.vectorColName = vectorColName;
		modelData.params.distanceType = distanceType;
		modelData.params.vectorSize = vectorSize;
		modelData.params.latitudeColName = latitudeColName;
		modelData.params.longtitudeColName = longtitudeColName;

		RowCollector collector = new RowCollector();
		new KMeansModelDataConverter().save(modelData, collector);
		return collector.getRows();
	}
}
