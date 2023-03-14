package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansAssignCluster;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansInitCentroids;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansIterTermination;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansModelDataConverter;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansOutputModel;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansPreallocateCentroid;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansUpdateCentroids;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.operator.batch.statistics.utils.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.params.clustering.KMeansTrainParams;
import com.alibaba.alink.params.shared.clustering.HasKMeansWithHaversineDistanceType;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * k-mean clustering is a method of vector quantization, originally from signal processing, that is popular for cluster
 * analysis in data mining. k-mean clustering aims to partition n observations into k clusters in which each
 * observation belongs to the cluster with the nearest mean, serving as a prototype of the cluster.
 * <p>
 * (https://en.wikipedia.org/wiki/K-means_clustering)
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {
	@PortSpec(value = PortType.MODEL, desc = PortDesc.KMEANS_MODEL),
})
@ParamSelectColumnSpec(name = "vectorCol", portIndices = 0, allowedTypeCollections = {TypeCollections.VECTOR_TYPES})
@NameCn("K均值聚类训练")
@NameEn("KMeans Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.clustering.KMeans")
public final class KMeansTrainBatchOp extends BatchOperator <KMeansTrainBatchOp>
	implements KMeansTrainParams <KMeansTrainBatchOp>,
	WithModelInfoBatchOp <KMeansModelInfoBatchOp.KMeansModelInfo, KMeansTrainBatchOp, KMeansModelInfoBatchOp> {

	public static final String TRAIN_DATA = "trainData";
	public static final String INIT_CENTROID = "initCentroid";
	public static final String CENTROID1 = "centroid1";
	public static final String CENTROID2 = "centroid2";
	public static final String CENTROID_ALL_REDUCE = "centroidAllReduce";
	public static final String KMEANS_STATISTICS = "statistics";
	public static final String VECTOR_SIZE = "vectorSize";
	public static final String K = "k";
	private static final long serialVersionUID = -1848822118021355321L;

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

	@Override
	public KMeansModelInfoBatchOp getModelInfoBatchOp() {
		return new KMeansModelInfoBatchOp(this.getParams()).linkFrom(this);
	}

	static DataSet <Row> iterateICQ(DataSet <FastDistanceMatrixData> initCentroid,
									DataSet <FastDistanceVectorData> data,
									final DataSet <Integer> statistics,
									final int maxIter,
									final double tol,
									final FastDistance distance,
									HasKMeansWithHaversineDistanceType.DistanceType distanceType,
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
		final DistanceType distanceType = getDistanceType();
		FastDistance distance = distanceType.getFastDistance();

		Tuple2 <DataSet <Vector>, DataSet <BaseVectorSummary>> statistics =
			StatisticsHelper.summaryHelper(in, null, vectorColName);

		DataSet <Integer> vectorSize = statistics.f1.map(new MapFunction <BaseVectorSummary, Integer>() {
			private static final long serialVersionUID = 4184586558834055401L;

			@Override
			public Integer map(BaseVectorSummary value) {
				AkPreconditions.checkArgument(value.count() > 0,
					new AkIllegalDataException("The train dataset is empty!"));
				return value.vectorSize();
			}
		});

		DataSet <FastDistanceVectorData> data = statistics.f0.rebalance().map(
			new RichMapFunction<Vector, FastDistanceVectorData>() {
				private static final long serialVersionUID = -7443226889326704768L;
				private int vectorSize;
				@Override
				public void open(Configuration params){
					vectorSize = (int)this.getRuntimeContext().getBroadcastVariable(VECTOR_SIZE).get(0);
				}
				@Override
				public FastDistanceVectorData map(Vector value) {
					if(value instanceof SparseVector){
						((SparseVector)value).setSize(vectorSize);
					}
					return distance.prepareVectorData(Row.of(value), 0);
				}
			}).withBroadcastSet(vectorSize, VECTOR_SIZE);

		DataSet <FastDistanceMatrixData> initCentroid = KMeansInitCentroids.initKmeansCentroids(
			data,
			distance,
			this.getParams(),
			vectorSize,
			getRandomSeed());

		DataSet <Row> finalCentroid = iterateICQ(initCentroid, data,
			vectorSize, maxIter, tol, distance,
			HasKMeansWithHaversineDistanceType.DistanceType.valueOf(distanceType.name()), vectorColName, null, null);

		this.setOutput(finalCentroid, new KMeansModelDataConverter().getModelSchema());

		return this;
	}

}
