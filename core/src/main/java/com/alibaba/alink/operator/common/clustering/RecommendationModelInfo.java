package com.alibaba.alink.operator.common.clustering;

import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;

import java.io.Serializable;

/**
 * RecommendationModelSummary is the base summary class of recommendation model.
 */
public abstract class RecommendationModelInfo implements Serializable {
	private static final long serialVersionUID = -9141825751591142318L;

	public abstract int getUserNumber();

	public abstract int getItemNumber();

	public abstract int getTotalSamples();

	protected String modelSummaryHead(String modelName) {
		StringBuilder sbd = new StringBuilder(PrettyDisplayUtils.displayHeadline(modelName + "ModelInfo", '-'));
		sbd.append("Recommendation training model: ")
			.append(modelName)
			.append("\n")
			.append(PrettyDisplayUtils.displayHeadline("Statistics", '='));
		String[][] table = new String[][] {{"Number of samples", Integer.toString(getTotalSamples())},
			{"Number of users", Integer.toString(getUserNumber())}, {"Number of items", Integer.toString(
			getItemNumber())}};
		sbd.append(PrettyDisplayUtils.displayTable(table, 3, 2, null, null, null));
		return sbd.toString();
	}
}
