package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.DistanceType;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.params.similarity.BaseJoinTopNLSHParams;
import com.alibaba.alink.params.feature.BaseLSHTrainParams;
import com.alibaba.alink.params.feature.HasNumHashTables;
import com.alibaba.alink.params.feature.HasNumProjectionsPerTable;
import com.alibaba.alink.params.feature.HasProjectionWidth;
import com.alibaba.alink.params.shared.HasMLEnvironmentId;
import com.alibaba.alink.params.shared.tree.HasSeed;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST;

/**
 * LSH is usually used to search nearest neighbor or join similar items. Here, we realized
 * the function of approxNearestNeighbors and approxSimilarityJoin for Euclidean distance
 * and Jaccard distance.
 */
public class LocalitySensitiveHashApproxFunctions {

	public static DataSet<BaseLSH> buildLSH(BatchOperator left, BatchOperator right, Params params){
		DistanceType distanceType = DistanceType.valueOf(params.get(BaseJoinTopNLSHParams.DISTANCE_TYPE).toUpperCase());

		Preconditions.checkArgument(
			TableUtil.findColIndex(left.getSchema(), params.get(BaseJoinTopNLSHParams.LEFT_ID_COL)) >= 0
				&& TableUtil.findColIndex(left.getSchema(), params.get(BaseJoinTopNLSHParams.LEFT_COL)) >= 0
				&& TableUtil.findColIndex(right.getSchema(), params.get(BaseJoinTopNLSHParams.RIGHT_ID_COL)) >= 0
				&& TableUtil.findColIndex(right.getSchema(), params.get(BaseJoinTopNLSHParams.RIGHT_COL)) >= 0,
			"Can not find given columns!");

		DataSet<BaseLSH> lsh;

		switch (distanceType) {
			case JACCARD: {
				lsh = MLEnvironmentFactory.get(params.get(HasMLEnvironmentId.ML_ENVIRONMENT_ID)).getExecutionEnvironment().fromElements(
					new MinHashLSH(params.get(BaseJoinTopNLSHParams.SEED),
						params.get(BaseJoinTopNLSHParams.NUM_PROJECTIONS_PER_TABLE),
						params.get(BaseJoinTopNLSHParams.NUM_HASH_TABLES)));
				break;
			}
			case EUCLIDEAN: {
				Tuple2<DataSet<Vector>, DataSet<BaseVectorSummary>> statistics =
					StatisticsHelper.summaryHelper(left, null, params.get(BaseJoinTopNLSHParams.LEFT_COL));
				lsh = statistics.f1.mapPartition(new MapPartitionFunction<BaseVectorSummary, BaseLSH>() {
					@Override
					public void mapPartition(Iterable<BaseVectorSummary> values, Collector<BaseLSH> out) {
						List<BaseVectorSummary> tensorInfo = new ArrayList<>();
						values.forEach(tensorInfo::add);
						out.collect(new BucketRandomProjectionLSH(params.get(BaseJoinTopNLSHParams.SEED),
							tensorInfo.get(0).vectorSize(),
							params.get(BaseJoinTopNLSHParams.NUM_PROJECTIONS_PER_TABLE),
							params.get(BaseJoinTopNLSHParams.NUM_HASH_TABLES),
							params.get(BaseJoinTopNLSHParams.PROJECTION_WIDTH)));
					}
				});
				break;
			}
			default: {
				throw new IllegalArgumentException("Not support " + distanceType);
			}
		}
		return lsh;
	}

	public static DataSet <Row> approxNearestNeighbors(DataSet <Row> query, DataSet <Row> dict, final int topN,
													   DataSet <BaseLSH> lsh) {
		DataSet <Tuple3 <Vector, Long, Row>> queryData = DataSetUtils.zipWithUniqueId(query).map(
			new InitData());
		DataSet <Tuple3 <Vector, Long, Row>> dictData = DataSetUtils.zipWithUniqueId(dict).map(
			new InitData());

		DataSet <Tuple3 <Integer, Integer, Long>> dictHashed = dictData
			.rebalance()
			.flatMap(new HashData())
			.withBroadcastSet(lsh, "lsh");

		DataSet <Tuple3 <Integer, Integer, Long>> queryHashed = queryData
			.rebalance()
			.flatMap(new HashData())
			.withBroadcastSet(lsh, "lsh");

		DataSet <Tuple2 <Long, Long>> queryDictIdData =(DataSet)queryHashed
			.joinWithHuge(dictHashed)
			.where(0, 1)
			.equalTo(0, 1)
			.projectFirst(2)
			.projectSecond(2)
			.distinct();

		DataSet <Tuple3 <Vector, Row, Long>> dictVecRowQueryId = dictData
			.join(queryDictIdData, REPARTITION_HASH_FIRST)
			.where(1)
			.equalTo(1)
			.projectFirst(0, 2)
			.projectSecond(0);

		//get the right row
		return queryData
			.join(dictVecRowQueryId, REPARTITION_HASH_FIRST)
			.where(1)
			.equalTo(2)
			.with(new JoinQueryData())
			.withBroadcastSet(lsh, "lsh")
			.withPartitioner(new CustomPartitioner())
			.groupBy(0)
			.reduceGroup(new ReduceTopN(topN));
	}

	public static DataSet <Row> approxSimilarityJoin(DataSet <Row> leftDataSet, DataSet <Row> rightDataSet,
													 final double threshold,
													 DataSet <BaseLSH> lsh) {
		DataSet <Tuple3 <Vector, Long, Row>> leftData = DataSetUtils.zipWithUniqueId(leftDataSet).map(
			new InitData());
		DataSet <Tuple3 <Vector, Long, Row>> rightData = DataSetUtils.zipWithUniqueId(rightDataSet).map(
			new InitData());

		DataSet <Tuple3 <Integer, Integer, Long>> rightHashed = rightData
			.rebalance()
			.flatMap(new HashData())
			.withBroadcastSet(lsh, "lsh");

		DataSet <Tuple3 <Integer, Integer, Long>> leftHashed = leftData
			.rebalance()
			.flatMap(new HashData())
			.withBroadcastSet(lsh, "lsh");

		DataSet <Tuple2 <Long, Long>> joinData = (DataSet)(leftHashed
			.joinWithHuge(rightHashed)
			.where(0, 1)
			.equalTo(0, 1)
			.projectFirst(2)
			.projectSecond(2)
			.distinct());

		DataSet <Tuple3 <Vector, Row, Long>> leftJoin = leftData
			.joinWithHuge(joinData)
			.where(1)
			.equalTo(0)
			.projectFirst(0, 2)
			.projectSecond(1);

		return rightData
			.joinWithHuge(leftJoin)
			.where(1)
			.equalTo(2)
			.with(new FinalJoin(threshold))
			.withBroadcastSet(lsh, "lsh")
			.withPartitioner(new CustomPartitioner());
	}

	/**
	 * Join the id table with the original data table.
	 */
	private static class JoinQueryData extends
		RichJoinFunction <Tuple3 <Vector, Long, Row>, Tuple3 <Vector, Row, Long>, Tuple4 <Long, Row, Row,
			Double>> {
		private BaseLSH lsh;

		@Override
		public void open(Configuration parameters) {
			List <BaseLSH> list = this.getRuntimeContext().getBroadcastVariable("lsh");
			this.lsh = list.get(0);
		}

		@Override
		public Tuple4 <Long, Row, Row, Double> join(Tuple3 <Vector, Long, Row> left,
														  Tuple3 <Vector, Row, Long> right) throws Exception {
			return Tuple4.of(left.f1, left.f2, right.f1, lsh.keyDistance(left.f0, right.f0));
		}
	}

	/**
	 * Calculate the real distance of vectors hashed into the same bucket and filter those whose distances are greater
	 * than threshold.
	 */
	private static class FinalJoin
		extends RichFlatJoinFunction<Tuple3 <Vector, Long, Row>, Tuple3 <Vector, Row, Long>, Row> {
		private double threshold;
		private BaseLSH lsh;

		public FinalJoin(double threshold) {
			this.threshold = threshold;
		}

		@Override
		public void open(Configuration parameters) {
			List <BaseLSH> list = this.getRuntimeContext().getBroadcastVariable("lsh");
			this.lsh = list.get(0);
		}

		@Override
		public void join(Tuple3 <Vector, Long, Row> row, Tuple3 <Vector, Row, Long> t, Collector <Row> collector)
			throws Exception {
			double d = lsh.keyDistance(row.f0, t.f0);
			if (d < threshold) {
				Row out = Row.of(t.f1.getField(0), row.f2.getField(0), d);
				collector.collect(out);
			}
		}
	}

	/**
	 * Get the topN nearest neighbor of one vector.
	 */
	private static class ReduceTopN implements GroupReduceFunction<Tuple4 <Long, Row, Row, Double>, Row> {
		private int topN;

		public ReduceTopN(int topN) {
			this.topN = topN;
		}

		@Override
		public void reduce(Iterable <Tuple4 <Long, Row, Row, Double>> iterable, Collector <Row> collector)
			throws Exception {
			Object id = null;
			List<Tuple2<Double, Row>> list = new ArrayList<>();

			for (Tuple4 <Long, Row, Row, Double> t : iterable) {
				if (null == id) {
					id = t.f1.getField(0);
				}
				list.add(Tuple2.of(t.f3, t.f2));
			}
			list.sort(new Comparator<Tuple2<Double, Row>>() {
				@Override
				public int compare(Tuple2<Double, Row> o1, Tuple2<Double, Row> o2) {
					return o1.f0.compareTo(o2.f0);
				}
			});
			long rank = 1L;
			for(int i = 0; i < Math.min(list.size(), topN); i++){
			    Tuple2<Double, Row> tuple = list.get(i);
                Row row = Row.of(id, tuple.f1.getField(0), tuple.f0, rank++);
                collector.collect(row);
            }
		}
	}

	/**
	 * Extract the id and vector from input row.
	 */
	private static class InitData implements MapFunction<Tuple2 <Long, Row>, Tuple3 <Vector, Long, Row>> {
		@Override
		public Tuple3 <Vector, Long, Row> map(Tuple2 <Long, Row> tuple) {
			Row row = tuple.f1;
			Vector vec = VectorUtil.getVector(row.getField(1));
			return Tuple3.of(vec, tuple.f0, Row.of(row.getField(0)));
		}
	}

	/**
	 * Hash the input vector into several buckets.
	 */
	private static class HashData
		extends RichFlatMapFunction<Tuple3 <Vector, Long, Row>, Tuple3 <Integer, Integer, Long>> {
		private final static Logger LOG = LoggerFactory.getLogger(HashData.class);
		private BaseLSH lsh;

		@Override
		public void open(Configuration parameters) {
			List<BaseLSH> list = this.getRuntimeContext().getBroadcastVariable("lsh");
			this.lsh = list.get(0);
			LOG.info(System.currentTimeMillis() + ": TaskId " + this.getRuntimeContext().getIndexOfThisSubtask()
				+ " hash begins.");
		}

		@Override
		public void close() {
			LOG.info(System.currentTimeMillis() + ": TaskId " + this.getRuntimeContext().getIndexOfThisSubtask()
				+ " hash ends.");
		}

		@Override
		public void flatMap(Tuple3 <Vector, Long, Row> tuple, Collector <Tuple3 <Integer, Integer, Long>> collector)
			throws Exception {
			DenseVector hashes = lsh.hashFunction(tuple.f0);
			for (int i = 0; i < hashes.size(); i++) {
				collector.collect(Tuple3.of(i, (int)hashes.get(i), tuple.f1));
			}
		}
	}

	/**
	 * Custom partition the data based on the index.
	 */
	private static class CustomPartitioner implements Partitioner<Long> {
		@Override
		public int partition(Long key, int numPartitions) {
			return (int) (key % numPartitions);
		}
	}
}
