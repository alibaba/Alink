package com.alibaba.alink.operator.batch.clustering;

import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.DistanceType;
import com.alibaba.alink.operator.common.clustering.kmeans.*;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.params.clustering.KMeansTrainParams;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * k-mean clustering is a method of vector quantization, originally from signal processing, that is popular for cluster
 * analysis in data mining. k-mean clustering aims to partition n observations into k clusters in which each
 * observation belongs to the cluster with the nearest mean, serving as a prototype of the cluster.
 * <p>
 * (https://en.wikipedia.org/wiki/K-means_clustering)
 */
public final class KMeansTrainBatchOp extends BatchOperator <KMeansTrainBatchOp>
	implements KMeansTrainParams <KMeansTrainBatchOp> {

	public static final String TRAIN_DATA = "trainData";
	public static final String INIT_CENTROID = "initCentroid";
	public static final String CENTROID1 = "centroid1";
	public static final String CENTROID2 = "centroid2";
	public static final String CENTROID_ALL_REDUCE = "centroidAllReduce";
	public static final String KMEANS_STATISTICS = "statistics";
	public static final String VECTOR_SIZE = "vectorSize";
	public static final String K = "k";

	/**
	 * null constructor.
	 */
	public KMeansTrainBatchOp() {
		this(null);
	}

	/**
	 * constructor.
	 * * @param params the parameters set.
	 */
	public KMeansTrainBatchOp(Params params) {
		super(params);
	}

	static DataSet <Row> iterateICQ(DataSet <FastDistanceMatrixData> initCentroid,
									DataSet <FastDistanceVectorData> data,
			   						final DataSet <Integer> statistics,
			   						final int maxIter,
			   						final double tol,
			   						final FastDistance distance,
									DistanceType distanceType,
			   						final String vectorColName,
			   						final String latitudeColName,
			   						final String longitudeColName) {

		return new IterativeComQueue()
			.initWithPartitionedData(TRAIN_DATA, data)
			.initWithBroadcastData(INIT_CENTROID, initCentroid)
			.initWithBroadcastData(KMEANS_STATISTICS, statistics)
			.add(new KMeansPreallocateCentroid())
			.add(new KMeansAssignCluster(distance))
			.add(new AllReduce(CENTROID_ALL_REDUCE))
			.add(new KMeansUpdateCentroids(distance))
			.setCompareCriterionOfNode0(new KMeansIterTermination(distance, tol))
			.closeWith(new KMeansOutputModel(distanceType, vectorColName, latitudeColName, longitudeColName))
			.setMaxIter(maxIter)
			.exec();
	}

	@Override
	public KMeansTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		final int maxIter = this.getMaxIter();
		final double tol = this.getEpsilon();
		final String vectorColName = this.getVectorCol();
		final DistanceType distanceType = DistanceType.valueOf(this.getDistanceType().toUpperCase());
		Preconditions.checkArgument(distanceType == DistanceType.EUCLIDEAN || distanceType == DistanceType.COSINE,
			"distanceType %s not support!", distanceType.name());
		FastDistance distance = (FastDistance)distanceType.getContinuousDistance();

		Tuple2 <DataSet <Vector>, DataSet <BaseVectorSummary>> statistics =
			StatisticsHelper.summaryHelper(in, null, vectorColName);

		DataSet <FastDistanceVectorData> data = statistics.f0.rebalance().map(
			new MapFunction <Vector, FastDistanceVectorData>() {
				@Override
				public FastDistanceVectorData map(Vector value) {
					return distance.prepareVectorData(Row.of(value), 0);
				}
			});

		DataSet <Integer> vectorSize = statistics.f1.map(new MapFunction <BaseVectorSummary, Integer>() {
			@Override
			public Integer map(BaseVectorSummary value) {
				Preconditions.checkArgument(value.count() > 0, "The train dataset is empty!");
				return value.vectorSize();
			}
		});

		DataSet <FastDistanceMatrixData> initCentroid = KMeansInitCentroids.initKmeansCentroids(data, distance, this.getParams(), vectorSize);

		DataSet <Row> finalCentroid = iterateICQ(initCentroid, data,
			vectorSize, maxIter, tol, distance, distanceType, vectorColName, null, null);

		this.setOutput(finalCentroid, new KMeansModelDataConverter().getModelSchema());

		return this;
	}
}
