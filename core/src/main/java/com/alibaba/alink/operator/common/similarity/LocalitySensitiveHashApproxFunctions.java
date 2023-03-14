package com.alibaba.alink.operator.common.similarity;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.similarity.lsh.BaseLSH;
import com.alibaba.alink.operator.common.similarity.lsh.BucketRandomProjectionLSH;
import com.alibaba.alink.operator.common.similarity.lsh.MinHashLSH;
import com.alibaba.alink.operator.batch.statistics.utils.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.params.shared.HasMLEnvironmentId;
import com.alibaba.alink.params.similarity.VectorApproxNearestNeighborTrainParams;

import java.util.ArrayList;
import java.util.List;

/**
 * LSH is usually used to search nearest neighbor or join similar items. Here, we realized the function of
 * approxNearestNeighbors and approxSimilarityJoin for Euclidean distance and Jaccard distance.
 */
public class LocalitySensitiveHashApproxFunctions {
	public static BaseLSH buildLSH(Params params, int vectorSize) {
		VectorApproxNearestNeighborTrainParams.Metric metric = params
			.get(VectorApproxNearestNeighborTrainParams.METRIC);

		long seed = params.get(VectorApproxNearestNeighborTrainParams.SEED);
		int numProjectionsPerTable = params.get(VectorApproxNearestNeighborTrainParams.NUM_PROJECTIONS_PER_TABLE);
		int numHashTables = params.get(VectorApproxNearestNeighborTrainParams.NUM_HASH_TABLES);

		switch (metric) {
			case JACCARD: {
				return new MinHashLSH(seed, numProjectionsPerTable, numHashTables);
			}
			case EUCLIDEAN: {
				return new BucketRandomProjectionLSH(seed, vectorSize,
					numProjectionsPerTable, numHashTables,
					params.get(VectorApproxNearestNeighborTrainParams.PROJECTION_WIDTH));
			}
			default: {
				throw new AkUnsupportedOperationException("Metric not supported: " + metric);
			}
		}
	}

	public static DataSet <BaseLSH> buildLSH(BatchOperator in, Params params, String vectorCol) {
		DataSet <BaseLSH> lsh;
		VectorApproxNearestNeighborTrainParams.Metric metric = params.get(
			VectorApproxNearestNeighborTrainParams.METRIC);

		switch (metric) {
			case JACCARD: {
				lsh = MLEnvironmentFactory.get(params.get(HasMLEnvironmentId.ML_ENVIRONMENT_ID))
					.getExecutionEnvironment().fromElements(
						new MinHashLSH(params.get(VectorApproxNearestNeighborTrainParams.SEED),
							params.get(VectorApproxNearestNeighborTrainParams.NUM_PROJECTIONS_PER_TABLE),
							params.get(VectorApproxNearestNeighborTrainParams.NUM_HASH_TABLES)));
				break;
			}
			case EUCLIDEAN: {
				Tuple2 <DataSet <Vector>, DataSet <BaseVectorSummary>> statistics =
					StatisticsHelper.summaryHelper(in, null, vectorCol);
				lsh = statistics.f1.mapPartition(new MapPartitionFunction <BaseVectorSummary, BaseLSH>() {
					private static final long serialVersionUID = -3698577489884292933L;

					@Override
					public void mapPartition(Iterable <BaseVectorSummary> values, Collector <BaseLSH> out) {
						List <BaseVectorSummary> tensorInfo = new ArrayList <>();
						values.forEach(tensorInfo::add);
						out.collect(
							new BucketRandomProjectionLSH(params.get(VectorApproxNearestNeighborTrainParams.SEED),
								tensorInfo.get(0).vectorSize(),
								params.get(VectorApproxNearestNeighborTrainParams.NUM_PROJECTIONS_PER_TABLE),
								params.get(VectorApproxNearestNeighborTrainParams.NUM_HASH_TABLES),
								params.get(VectorApproxNearestNeighborTrainParams.PROJECTION_WIDTH)));
					}
				});
				break;
			}
			default: {
				throw new IllegalArgumentException("Not support " + metric);
			}
		}
		return lsh;
	}
}
