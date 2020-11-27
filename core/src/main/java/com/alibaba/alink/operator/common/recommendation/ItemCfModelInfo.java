package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.clustering.RecommendationModelInfo;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import com.alibaba.alink.params.recommendation.ItemCfRecommTrainParams;

import java.util.List;
import java.util.Set;

public class ItemCfModelInfo extends RecommendationModelInfo {
	private static final long serialVersionUID = -805475176660338849L;
	/**
	 * Distinct user number.
	 */
	private int userNum;

	/**
	 * Distinct item number.
	 */
	private int itemNum;

	/**
	 * sample number.
	 */
	private int totalSamples;

	/**
	 * Params
	 */
	private Params meta;

	public ItemCfModelInfo(List <Row> list) {
		ItemCfRecommData data = new ItemCfRecommModelDataConverter(RecommType.ITEMS_PER_USER).load(list);
		this.userNum = data.userItems.size();
		this.itemNum = data.items.length;
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
		StringBuilder sbd = new StringBuilder(modelSummaryHead("ItemCf"));
		sbd.append(PrettyDisplayUtils.displayHeadline("Model Parameters", '='));
		String[][] table = new String[][] {{"SimilarityType", meta.get(ItemCfRecommTrainParams.SIMILARITY_TYPE).name
			()},
			{"SimilarityThreshold", meta.get(ItemCfRecommTrainParams.SIMILARITY_THRESHOLD).toString()},
			{"MaxNeighborNumber", meta.get(ItemCfRecommTrainParams.MAX_NEIGHBOR_NUMBER).toString()}};
		sbd.append(PrettyDisplayUtils.displayTable(table, 3, 2, null, null, null));
		return sbd.toString();
	}

}
