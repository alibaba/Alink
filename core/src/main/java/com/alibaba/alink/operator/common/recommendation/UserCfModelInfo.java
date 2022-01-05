package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.clustering.RecommendationModelInfo;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import com.alibaba.alink.params.recommendation.ItemCfRecommTrainParams;

import java.util.List;
import java.util.Set;

public class UserCfModelInfo extends RecommendationModelInfo {
	private static final long serialVersionUID = 6270910924435836349L;
	/**
	 * Distinct user number.
	 */
	private final int userNum;

	/**
	 * Distinct item number.
	 */
	private final int itemNum;

	/**
	 * sample number.
	 */
	private final int totalSamples;

	/**
	 * Params
	 */
	private final Params meta;

	public UserCfModelInfo(List <Row> list) {
		ItemCfRecommData data = new ItemCfRecommModelDataConverter(RecommType.ITEMS_PER_USER).load(list);
		this.itemNum = data.userItems.size();
		this.userNum = data.items.length;
		this.totalSamples = data.userItems.values().stream().mapToInt(Set::size).sum();
		this.meta = data.meta;
	}

	@Override
	public int getUserNumber() {
		return userNum;
	}

	@Override
	public int getItemNumber() {
		return itemNum;
	}

	@Override
	public int getTotalSamples() {
		return totalSamples;
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder(modelSummaryHead("UserCf"));
		sbd.append(PrettyDisplayUtils.displayHeadline("Model Parameters", '='));
		String[][] table = new String[][] {{"SimilarityType", meta.get(ItemCfRecommTrainParams.SIMILARITY_TYPE).name
			()},
			{"SimilarityThreshold", meta.get(ItemCfRecommTrainParams.SIMILARITY_THRESHOLD).toString()},
			{"MaxNeighborNumber", meta.get(ItemCfRecommTrainParams.MAX_NEIGHBOR_NUMBER).toString()}};
		sbd.append(PrettyDisplayUtils.displayTable(table, 3, 2, null, null, null));
		return sbd.toString();
	}

}
