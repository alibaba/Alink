package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.kmodes.KModesModel;
import com.alibaba.alink.operator.common.clustering.kmodes.KModesModelData;
import com.alibaba.alink.operator.common.distance.OneZeroDistance;
import com.alibaba.alink.params.clustering.KModesTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Partitioning a large set of objects into homogeneous clusters is a fundamental operation in data mining. The k-mean
 * algorithm is best suited for implementing this operation because of its efficiency in clustering large data sets.
 * However, working only on numeric values limits its use in data mining because data sets in data mining often contain
 * categorical values. In this paper we present an algorithm, called k-modes, to extend the k-mean paradigm to
 * categorical domains. We introduce new dissimilarity measures to deal with categorical objects, replace mean of
 * clusters with modes, and use a frequency based method to update modes in the clustering process to minimise the
 * clustering cost function. Tested with the well known soybean disease data set the algorithm has demonstrated a very
 * good classification performance. Experiments on a very large health insurance data set consisting of half a million
 * records and 34 categorical attributes show that the algorithm is scalable in terms of both the number of clusters and
 * the number of records.
 * <p>
 * Huang, Zhexue. "A fast clustering algorithm to cluster very large categorical data sets in data mining." DMKD 3.8
 * (1997): 34-39.
 *
 * @author guotao.gt
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.MODEL)})
@ParamSelectColumnSpec(name = "featureCols", portIndices = 0, allowedTypeCollections = {TypeCollections.NUMERIC_TYPES})
@NameCn("Kmodes训练")
@NameEn("KModes Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.clustering.KModes")
public final class KModesTrainBatchOp extends BatchOperator <KModesTrainBatchOp>
	implements KModesTrainParams <KModesTrainBatchOp>{

	private static final long serialVersionUID = 7392501340000162512L;

	/**
	 * null constructor.
	 */
	public KModesTrainBatchOp() {
		super(new Params());
	}

	/**
	 * this constructor has all parameter
	 *
	 * @param params
	 */
	public KModesTrainBatchOp(Params params) {
		super(params);
	}

	/**
	 * union two map,the same key will plus the value
	 *
	 * @param kv1 the 1st kv map
	 * @param kv2 the 2nd kv map
	 * @return the map united
	 */
	private static Map <String, Integer> unionMaps(Map <String, Integer> kv1, Map <String, Integer> kv2) {
		Map <String, Integer> kv = new HashMap <>();
		kv.putAll(kv1);
		for (String k : kv2.keySet()) {
			if (kv.containsKey(k)) {
				kv.put(k, kv.get(k) + kv2.get(k));
			} else {
				kv.put(k, kv2.get(k));
			}
		}
		return kv;
	}

	/**
	 * union two map array
	 *
	 * @param kvArray1 the 1st kv array map
	 * @param kvArray2 the 2nd kv array map
	 * @param dim      the array's length
	 * @return the map array united
	 */
	private static Map <String, Integer>[] unionMaps(Map <String, Integer>[] kvArray1, Map <String, Integer>[]
		kvArray2,
													 int dim) {
		Map <String, Integer>[] kvArray = new HashMap[dim];
		for (int i = 0; i < dim; i++) {
			kvArray[i] = unionMaps(kvArray1[i], kvArray2[i]);
		}
		return kvArray;
	}

	/**
	 * get the max key of map
	 *
	 * @param kv the kv map
	 * @return the max key of map
	 */
	private static String getKOfMaxV(Map <String, Integer> kv) {
		Integer tmp = Integer.MIN_VALUE;
		String k = null;
		for (Map.Entry <String, Integer> entry : kv.entrySet()) {
			if (entry.getValue() > tmp) {
				tmp = entry.getValue();
				k = entry.getKey();
			}
		}
		return k;
	}

	/**
	 * get the max keys of map array
	 *
	 * @param kvs the kv map array
	 * @return the max keys of map array
	 */
	private static String[] getKOfMaxV(Map <String, Integer> kvs[]) {
		String[] ks = new String[kvs.length];
		for (int i = 0; i < kvs.length; i++) {
			ks[i] = getKOfMaxV(kvs[i]);
		}
		return ks;
	}

	@Override
	public KModesTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		// get the input parameter's value
		final String[] featureColNames = (this.getParams().contains(FEATURE_COLS)
			&& this.getFeatureCols() != null
			&& this.getFeatureCols().length > 0) ?
			this.getFeatureCols() : in.getSchema().getFieldNames();

		final int numIter = this.getNumIter();
		final int k = this.getK();

		if ((featureColNames == null || featureColNames.length == 0)) {
			throw new RuntimeException("featureColNames should be set !");
		}

		// construct the sql to get the input feature column name
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < featureColNames.length; i++) {
			if (i > 0) {
				sbd.append(", ");
			}
			sbd.append("cast(`")
				.append(featureColNames[i])
				.append("` as VARCHAR) as `")
				.append(featureColNames[i])
				.append("`");
		}

		// get the input data needed
		DataSet <String[]> data = in.select(sbd.toString()).getDataSet()
			.map(new MapFunction <Row, String[]>() {
				private static final long serialVersionUID = 8380190916941374707L;

				@Override
				public String[] map(Row row) throws Exception {
					String[] values = new String[row.getArity()];
					for (int i = 0; i < values.length; i++) {
						values[i] = (String) row.getField(i);
					}
					return values;
				}
			});

		/**
		 * initial the centroid
		 * Tuple3: clusterId, clusterWeight, clusterCentroid
		 */
		DataSet <Tuple3 <Long, Double, String[]>> initCentroid = DataSetUtils
			.zipWithIndex(DataSetUtils.sampleWithSize(data, false, k))
			.map(new MapFunction <Tuple2 <Long, String[]>, Tuple3 <Long, Double, String[]>>() {
				private static final long serialVersionUID = -6852532761276146862L;

				@Override
				public Tuple3 <Long, Double, String[]> map(Tuple2 <Long, String[]> v)
					throws Exception {
					return new Tuple3 <>(v.f0, 0., v.f1);
				}
			})
			.withForwardedFields("f0->f0;f1->f2");

		IterativeDataSet <Tuple3 <Long, Double, String[]>> loop = initCentroid.iterate(numIter);
		DataSet <Tuple2 <Long, String[]>> samplesWithClusterId = assignClusterId(data, loop);
		DataSet <Tuple3 <Long, Double, String[]>> updatedCentroid = updateCentroid(samplesWithClusterId,
			k, featureColNames.length);
		DataSet <Tuple3 <Long, Double, String[]>> finalCentroid = loop.closeWith(updatedCentroid);

		// map the final centroid to row type, plus with the meta info
		DataSet <Row> modelRows = finalCentroid
			.mapPartition(new MapPartitionFunction <Tuple3 <Long, Double, String[]>, Row>() {
				private static final long serialVersionUID = -3961032097333930998L;

				@Override
				public void mapPartition(Iterable <Tuple3 <Long, Double, String[]>> iterable,
										 Collector <Row> out) throws Exception {
					KModesModelData modelData = new KModesModelData();
					modelData.centroids = new ArrayList <>();
					for (Tuple3 <Long, Double, String[]> t : iterable) {
						modelData.centroids.add(t);
					}
					modelData.featureColNames = featureColNames;

					// meta plus data
					new KModesModel().save(modelData, out);
				}
			})
			.setParallelism(1);

		// store the clustering model to the table
		this.setOutput(modelRows, new KModesModel().getModelSchema());

		return this;
	}

	/**
	 * assign clusterId to sample
	 *
	 * @param data      the whole sample data
	 * @param centroids the centroids of clusters
	 * @return the DataSet of sample with clusterId
	 */
	private DataSet <Tuple2 <Long, String[]>> assignClusterId(
		DataSet <String[]> data,
		DataSet <Tuple3 <Long, Double, String[]>> centroids) {

		class FindClusterOp extends RichMapFunction <String[], Tuple2 <Long, String[]>> {
			private static final long serialVersionUID = 6305282153314372806L;
			List <Tuple3 <Long, Double, String[]>> centroids;
			OneZeroDistance distance;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				centroids = getRuntimeContext().getBroadcastVariable("centroids");
				this.distance = new OneZeroDistance();
			}

			@Override
			public Tuple2 <Long, String[]> map(String[] denseVector) throws Exception {
				long clusterId = KModesModel.findCluster(centroids, denseVector, distance);
				return new Tuple2 <>(clusterId, denseVector);
			}
		}

		return data.map(new FindClusterOp())
			.withBroadcastSet(centroids, "centroids");
	}

	/**
	 * update centroid of cluster
	 *
	 * @param samplesWithClusterId sample with clusterId
	 * @param k                    the number of clusters
	 * @param dim                  the vectorSize of featureColNames
	 * @return the DataSet of center with clusterId and the weight(the number of samples belong to the cluster)
	 */
	private DataSet <Tuple3 <Long, Double, String[]>> updateCentroid(
		DataSet <Tuple2 <Long, String[]>> samplesWithClusterId, final int k, final int dim) {

		// tuple3: clusterId, clusterWeight, clusterCentroid
		DataSet <Tuple3 <Long, Double, Map <String, Integer>[]>> localAggregate =
			samplesWithClusterId.mapPartition(new DataPartition(k, dim));

		return localAggregate
			.groupBy(0)
			.reduce(new DataReduce(dim))
			.map(
				new MapFunction <Tuple3 <Long, Double, Map <String, Integer>[]>, Tuple3 <Long, Double,
					String[]>>() {
					private static final long serialVersionUID = -6833217715929845251L;

					@Override
					public Tuple3 <Long, Double, String[]> map(
						Tuple3 <Long, Double, Map <String, Integer>[]> in) {
						return new Tuple3 <>(in.f0, in.f1, getKOfMaxV(in.f2));
					}
				})
			.withForwardedFields("f0;f1");
	}

	/**
	 * calc local centroids
	 */
	public static class DataPartition
		implements MapPartitionFunction <Tuple2 <Long, String[]>, Tuple3 <Long, Double, Map <String, Integer>[]>> {

		private static final long serialVersionUID = 4053491536690724820L;
		private int k;
		private int dim;

		public DataPartition(int k, int dim) {
			this.k = k;
			this.dim = dim;
		}

		@Override
		public void mapPartition(Iterable <Tuple2 <Long, String[]>> iterable,
								 Collector <Tuple3 <Long, Double, Map <String, Integer>[]>> collector)
			throws Exception {
			Map <String, Integer>[][] localCentroids = new HashMap[k][dim];
			for (int i = 0; i < k; i++) {
				for (int j = 0; j < dim; j++) {
					localCentroids[i][j] = new HashMap <String, Integer>(32);
				}
			}
			double[] localCounts = new double[k];
			Arrays.fill(localCounts, 0.);

			for (Tuple2 <Long, String[]> point : iterable) {
				int clusterId = point.f0.intValue();
				localCounts[clusterId] += 1.0;

				for (int j = 0; j < dim; j++) {
					if (localCentroids[clusterId][j].containsKey(point.f1[j])) {
						localCentroids[clusterId][j].put(point.f1[j],
							localCentroids[clusterId][j].get(point.f1[j]) + 1);
					} else {
						localCentroids[clusterId][j].put(point.f1[j], 1);
					}
				}
			}

			for (int i = 0; i < localCentroids.length; i++) {
				collector.collect(new Tuple3 <>((long) i, localCounts[i], localCentroids[i]));
			}
		}
	}

	/**
	 * calc Global Centroids
	 */
	public static class DataReduce implements ReduceFunction <Tuple3 <Long, Double, Map <String, Integer>[]>> {

		private static final long serialVersionUID = -7472289261425686956L;
		private int dim;

		public DataReduce(int dim) {
			this.dim = dim;
		}

		@Override
		public Tuple3 <Long, Double, Map <String, Integer>[]> reduce(Tuple3 <Long, Double, Map <String, Integer>[]>
																		 in1,
																	 Tuple3 <Long, Double, Map <String, Integer>[]>
																		 in2) {
			return new Tuple3 <>(in1.f0, in1.f1 + in2.f1, unionMaps(in1.f2, in2.f2, dim));
		}
	}
}
