package com.alibaba.alink.operator.common.clustering.kmeans;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.utils.Functional;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.params.clustering.KMeansTrainParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;
import static org.apache.flink.shaded.guava18.com.google.common.hash.Hashing.murmur3_128;

/**
 * Initialize a set of cluster centers using the k-means|| algorithm by Bahmani et al. (Bahmani et al., Scalable
 * K-Means++, VLDB 2012). This is a variant of k-means++ that tries to find dissimilar cluster centers by starting with
 * a random center and then doing passes where more centers are chosen with probability proportional to their squared
 * distance to the current cluster set. It results in a provable approximation to an optimal clustering.
 * <p>
 * The original paper can be found at http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf.
 */
public class KMeansInitCentroids implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(KMeansInitCentroids.class);
	private static final String CENTER = "centers";
	private static final String SUM_COSTS = "sumCosts";
	private static final String VECTOR_SIZE = "vectorSize";
	private static final long serialVersionUID = -8219073698493308787L;

	public static DataSet <FastDistanceMatrixData> initKmeansCentroids(DataSet <FastDistanceVectorData> data,
																	   FastDistance distance, Params params,
																	   DataSet <Integer> vectorSize,
																	   int seed) {
		final InitMode initMode = params.get(KMeansTrainParams.INIT_MODE);
		final int initSteps = params.get(KMeansTrainParams.INIT_STEPS);
		final int k = params.get(KMeansTrainParams.K);

		DataSet <FastDistanceMatrixData> initCentroid;
		switch (initMode) {
			case RANDOM: {
				initCentroid = randomInit(data, k, distance, vectorSize, seed);
				break;
			}
			case K_MEANS_PARALLEL: {
				initCentroid = kMeansPlusPlusInit(data, k, initSteps, distance, vectorSize, seed);
				break;
			}
			default: {
				throw new IllegalArgumentException("Unknown init mode: " + initMode);
			}
		}

		return initCentroid;
	}

	/**
	 * Initialize a set of cluster centers by sampling without replacement from samples.
	 */
	private static DataSet <FastDistanceMatrixData> randomInit(DataSet <FastDistanceVectorData> data,
															   final int k,
															   final FastDistance distance,
															   DataSet <Integer> vectorSize,
															   int seed) {
		return selectTopK(k, seed, data,
			new Functional.SerializableFunction <FastDistanceVectorData, byte[]>() {
				private static final long serialVersionUID = 6092460932245165972L;

				@Override
				public byte[] apply(FastDistanceVectorData v) {
					return v.getVector().toBytes();
				}
			})
			.mapPartition(
				new RichMapPartitionFunction <Tuple2 <Long, FastDistanceVectorData>, FastDistanceMatrixData>() {
					private static final long serialVersionUID = 2012759243672199273L;

					@Override
					public void mapPartition(Iterable <Tuple2 <Long, FastDistanceVectorData>> values,
											 Collector <FastDistanceMatrixData> out) {
						List <Integer> list = this.getRuntimeContext().getBroadcastVariable(VECTOR_SIZE);
						int vectorSize = list.get(0);
						List <FastDistanceVectorData> vectorList = new ArrayList <>();
						values.forEach(v -> vectorList.add(v.f1));
						out.collect(KMeansUtil.buildCentroidsMatrix(vectorList, distance, vectorSize));
					}
				})
			.withBroadcastSet(vectorSize, VECTOR_SIZE)
			.setParallelism(1);
	}

	private static DataSet <FastDistanceMatrixData> kMeansPlusPlusInit(DataSet <FastDistanceVectorData> data,
																	   final int k,
																	   final int initSteps,
																	   final FastDistance distance,
																	   DataSet <Integer> vectorSize,
																	   int seed) {
		final HashFunction hashFunc = murmur3_128(seed);
		DataSet <Tuple2 <Long, FastDistanceVectorData>> dataWithId = data.map(
			new MapFunction <FastDistanceVectorData, Tuple2 <Long, FastDistanceVectorData>>() {
				private static final long serialVersionUID = 1539229008777267709L;

				@Override
				public Tuple2 <Long, FastDistanceVectorData> map(FastDistanceVectorData value) throws Exception {
					Long hashValue = hashFunc.hashUnencodedChars(value.toString()).asLong();
					return Tuple2.of(hashValue, value);
				}
			});

		//id, vectorData, nearestCenterId, nearestCenterDist, mark(center/data)
		DataSet <Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>> dataNeighborMark = dataWithId.map(
			new MapFunction <Tuple2 <Long, FastDistanceVectorData>,
				Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>>() {
				private static final long serialVersionUID = -8289894247468770813L;

				@Override
				public Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean> map(
					Tuple2 <Long, FastDistanceVectorData> value) {
					return Tuple5.of(value.f0, value.f1, -1L, Double.MAX_VALUE, false);
				}
			}).withForwardedFields("f0;f1");

		DataSet <Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>> centers = dataNeighborMark
			.maxBy(0)
			.map(new TransformToCenter())
			.withForwardedFields("f0;f1");

		dataNeighborMark = dataNeighborMark
			.map(new CalWeight(distance))
			.withBroadcastSet(centers, CENTER)
			.withForwardedFields("f0;f1;f4");

		IterativeDataSet <Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>> loop =
			dataNeighborMark.iterate(initSteps - 1);

		DataSet <Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>> dataOnly = loop.filter(new FilterData
			());

		DataSet <Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>> oldCenter = loop.filter(
			new FilterCenter());

		DataSet <Tuple1 <Double>> sumCosts = dataOnly. <Tuple1 <Double>>project(3).aggregate(SUM, 0);

		DataSet <Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>> newCenter = dataOnly
			.partitionCustom(new Partitioner <Long>() {
				private static final long serialVersionUID = 8742959167492464159L;

				@Override
				public int partition(Long key, int numPartitions) {
					return (int) (Math.abs(key) % numPartitions);
				}
			}, 0)
			.sortPartition(0, Order.DESCENDING)
			.filter(new FilterNewCenter(k, seed))
			.withBroadcastSet(sumCosts, SUM_COSTS)
			.name("kmeans_||_pick")
			.map(new TransformToCenter())
			.withForwardedFields("f0;f1");

		DataSet <Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>> updateData = dataOnly
			.map(new CalWeight(distance))
			.withBroadcastSet(newCenter, CENTER)
			.withForwardedFields("f0;f1;f4");

		DataSet <Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>> finalDataAndCenter = loop.closeWith(
			updateData.union(oldCenter));

		DataSet <Tuple2 <Long, FastDistanceVectorData>> finalCenters = finalDataAndCenter.filter(new FilterCenter())
			.project(0, 1);

		DataSet <Tuple2 <Long, FastDistanceVectorData>> weight = finalDataAndCenter
			.filter(new FilterData())
			.map(new MapFunction <Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>, Tuple2 <Long, Long>>
				() {
				private static final long serialVersionUID = -7230628651729304469L;

				@Override
				public Tuple2 <Long, Long> map(Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean> t) {
					return Tuple2.of(t.f2, 1L);
				}
			})
			.withForwardedFields("f2->f0")
			.groupBy(0)
			.aggregate(SUM, 1)
			.join(finalCenters)
			.where(0)
			.equalTo(0)
			.projectFirst(1)
			.projectSecond(1);

		return weight
			.mapPartition(new LocalKmeans(k, distance, seed))
			.withBroadcastSet(vectorSize, VECTOR_SIZE)
			.setParallelism(1);
	}

	public static <T> DataSet <Tuple2 <Long, T>> selectTopK(
		final int k,
		int seed,
		DataSet <T> data,
		final Functional.SerializableFunction <T, byte[]> func) {
		TypeInformation dataType = data.getType();

		final HashFunction hashFunc = murmur3_128(seed);

		return data.map(new RichMapFunction <T, Tuple2 <Long, T>>() {
			private static final long serialVersionUID = 6994623243686615646L;

			@Override
			public Tuple2 <Long, T> map(T value) throws Exception {
				Long hashValue = hashFunc.hashBytes(func.apply(value)).asLong();
				return Tuple2.of(hashValue, value);
			}
		}).returns(new TupleTypeInfo(Types.LONG, dataType))
			.mapPartition(new MapPartitionFunction <Tuple2 <Long, T>, TreeMapT>() {
				private static final long serialVersionUID = 5813015538018452614L;

				@Override
				public void mapPartition(Iterable <Tuple2 <Long, T>> values,
										 Collector <TreeMapT> out) throws Exception {
					TreeMapT queue = new TreeMapT();
					long head = Long.MAX_VALUE;
					for (Tuple2 <Long, T> t : values) {
						head = KMeansUtil.updateQueue(queue.treeMap, t.f0, t.f1, k, head);
					}
					out.collect(queue);
				}
			}).returns(TreeMapT.class)
			.reduce(new ReduceFunction <TreeMapT>() {
				private static final long serialVersionUID = -3472592245281912784L;

				@Override
				public TreeMapT reduce(TreeMapT value1, TreeMapT value2) throws Exception {
					if (value2.treeMap.size() == 0) {
						return value1;
					}
					if (value1.treeMap.size() == 0) {
						return value2;
					}
					long head = (long) value1.treeMap.lastEntry().getKey();
					for (Map.Entry <Long, T> entry : ((TreeMap <Long, T>) value2.treeMap).entrySet()) {
						head = KMeansUtil.updateQueue(value1.treeMap, entry.getKey(), entry.getValue(), k, head);
					}
					return value1;
				}
			}).returns(TreeMapT.class)
			.flatMap(new FlatMapFunction <TreeMapT, Tuple2 <Long, T>>() {
				private static final long serialVersionUID = 3317982387795941044L;

				@Override
				public void flatMap(TreeMapT value, Collector <Tuple2 <Long, T>> out)
					throws Exception {
					long cnt = 0;
					for (Map.Entry <Long, T> entry : ((TreeMap <Long, T>) value.treeMap).entrySet()) {
						out.collect(Tuple2.of(cnt++, entry.getValue()));
					}
				}
			}).returns(new TupleTypeInfo(Types.LONG, dataType));
	}

	static class TreeMapT<T extends Serializable> implements Serializable {
		private static final long serialVersionUID = -2350624942257559150L;
		public TreeMap <Long, T> treeMap;

		public TreeMapT() {
			this.treeMap = new TreeMap <>();
		}
	}

	private static class FilterData
		implements FilterFunction <Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>> {
		private static final long serialVersionUID = -7062845155572458129L;

		@Override
		public boolean filter(Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean> value)
			throws Exception {
			return !value.f4;
		}
	}

	private static class FilterCenter
		implements FilterFunction <Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>> {
		private static final long serialVersionUID = -8363415544217000362L;

		@Override
		public boolean filter(Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean> value)
			throws Exception {
			return value.f4;
		}
	}

	private static class FilterNewCenter
		extends RichFilterFunction <Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>> {
		private static final long serialVersionUID = -6433641767140518820L;
		private transient double costThre;
		private transient Random random;
		private int k;
		private int seed;

		FilterNewCenter(int k, int seed) {
			this.k = k;
			this.seed = seed;
		}

		@Override
		public void open(Configuration parameters) {
			random = new Random(seed);
			List <Tuple1 <Double>> bcCostSum = getRuntimeContext().getBroadcastVariable(SUM_COSTS);
			costThre = 2.0 * k / bcCostSum.get(0).f0;
		}

		@Override
		public boolean filter(Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean> value) {
			return random.nextDouble() < value.f3 * costThre;
		}
	}

	private static class TransformToCenter
		implements MapFunction <Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>,
		Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>> {
		private static final long serialVersionUID = 5589065815045593976L;

		@Override
		public Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean> map(
			Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean> value) throws Exception {
			value.f2 = -1L;
			value.f3 = Double.MAX_VALUE;
			value.f4 = true;
			return value;
		}
	}

	private static class CalWeight extends
		RichMapFunction <Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>,
			Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>> {
		private static final long serialVersionUID = 4828540656733702022L;
		private transient List <Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean>> centers;
		private FastDistance distance;

		CalWeight(FastDistance distance) {
			this.distance = distance;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			centers = getRuntimeContext().getBroadcastVariable(CENTER);
		}

		@Override
		public Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean> map(
			Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean> sample) throws Exception {
			for (Tuple5 <Long, FastDistanceVectorData, Long, Double, Boolean> c : centers) {
				if (c.f0.equals(sample.f0)) {
					sample.f4 = true;
				} else {
					double d = distance.calc(c.f1, sample.f1).get(0, 0);
					if (d < sample.f3) {
						sample.f2 = c.f0;
						sample.f3 = d;
					}
				}
			}
			return sample;
		}
	}

	private static class LocalKmeans
		extends RichMapPartitionFunction <Tuple2 <Long, FastDistanceVectorData>, FastDistanceMatrixData> {
		private static final long serialVersionUID = 3014142447237244585L;
		private FastDistance distance;
		private int k;
		private transient int vectorSize;
		private int seed;

		LocalKmeans(int k, FastDistance distance, int seed) {
			this.k = k;
			this.distance = distance;
			this.seed = seed;
		}

		@Override
		public void open(Configuration parameters) {
			LOG.info("TaskId {} Local Kmeans begins!",
				this.getRuntimeContext().getIndexOfThisSubtask());
			List <Integer> list = this.getRuntimeContext().getBroadcastVariable(VECTOR_SIZE);
			vectorSize = list.get(0);
		}

		@Override
		public void close() {
			LOG.info("TaskId {} Local Kmeans ends!",
				this.getRuntimeContext().getIndexOfThisSubtask());
		}

		@Override
		public void mapPartition(Iterable <Tuple2 <Long, FastDistanceVectorData>> values,
								 Collector <FastDistanceMatrixData> out) throws
			Exception {
			List <Tuple2 <Long, FastDistanceVectorData>> list = new ArrayList <>();
			values.forEach(list::add);
			list.sort(Comparator.comparingLong(v -> v.f0));

			List <Long> sampleWeightsList = new ArrayList <>();
			List <FastDistanceVectorData> samples = new ArrayList <>();
			list.forEach(v -> {
				sampleWeightsList.add(v.f0);
				samples.add(v.f1);
			});

			if (samples.size() <= k) {
				out.collect(KMeansUtil.buildCentroidsMatrix(samples, distance, vectorSize));
			} else {
				long[] sampleWeights = new long[sampleWeightsList.size()];
				for (int i = 0; i < sampleWeights.length; i++) {
					sampleWeights[i] = sampleWeightsList.get(i);
				}
				out.collect(LocalKmeansFunc
					.kmeans(k, sampleWeights, samples.toArray(new FastDistanceVectorData[0]), distance, vectorSize,
						seed));
			}
		}
	}
}
