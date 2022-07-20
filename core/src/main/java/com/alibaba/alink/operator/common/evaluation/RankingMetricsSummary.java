package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkPreconditions;

public final class RankingMetricsSummary implements BaseMetricsSummary <RankingMetrics, RankingMetricsSummary> {

	private static final long serialVersionUID = 7096507489597992319L;
	/**
	 * Precision at k.
	 */
	double[] precisionArray;

	/**
	 * Ndcg at k.
	 */
	double[] ndcgArray;

	/**
	 * Recall at k.
	 */
	double[] recallArray;

	/**
	 * Map.
	 */
	double map;

	/**
	 * The count of samples.
	 */
	long total = 0L;

	/**
	 * Hit Rank
	 */
	double hitRank;

	/**
	 * Hit rate.
	 */
	int hits;

	MultiLabelMetricsSummary multiLabelMetricsSummary;

	public RankingMetricsSummary(long total,
								 double[] precisionArray,
								 double[] ndcgArray,
								 double map,
								 MultiLabelMetricsSummary multiLabelMetricsSummary,
								 int hits,
								 double hitRank,
								 double[] recallArray) {
		AkPreconditions.checkNotNull(multiLabelMetricsSummary,
			new AkIllegalDataException("MultiLabelMetrics is null, please check the input data!"));
		this.precisionArray = precisionArray;
		this.recallArray = recallArray;
		this.ndcgArray = ndcgArray;
		this.map = map;
		this.total = total;
		this.multiLabelMetricsSummary = multiLabelMetricsSummary;
		this.hits = hits;
		this.hitRank = hitRank;
	}

	@Override
	public RankingMetricsSummary merge(RankingMetricsSummary other) {
		if (null == other) {
			return this;
		}
		for (int i = 0; i < precisionArray.length; i++) {
			precisionArray[i] += other.precisionArray[i];
			ndcgArray[i] += other.ndcgArray[i];
			recallArray[i] += other.recallArray[i];
		}
		map += other.map;
		total += other.total;
		multiLabelMetricsSummary.merge(other.multiLabelMetricsSummary);
		hits += other.hits;
		hitRank += other.hitRank;
		return this;
	}

	@Override
	public RankingMetrics toMetrics() {
		Params params = new Params();
		for (int i = 0; i < precisionArray.length; i++) {
			precisionArray[i] /= total;
			ndcgArray[i] /= total;
			recallArray[i] /= total;
		}
		params.set(RankingMetrics.MAP, map / total);
		params.set(RankingMetrics.PRECISION_ARRAY, precisionArray);
		params.set(RankingMetrics.RECALL_ARRAY, recallArray);
		params.set(RankingMetrics.NDCG_ARRAY, ndcgArray);
		params.merge(multiLabelMetricsSummary.toMetrics().params);
		params.set(RankingMetrics.HIT_RATE, hits * 1.0 / total);
		params.set(RankingMetrics.AVE_RECIPRO_HIT_RANK, hitRank / total);
		return new RankingMetrics(params);
	}
}
