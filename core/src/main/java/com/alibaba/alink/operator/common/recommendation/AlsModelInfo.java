package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.clustering.RecommendationModelInfo;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import com.alibaba.alink.params.recommendation.AlsTrainParams;

import java.util.HashMap;
import java.util.Map;

public class AlsModelInfo extends RecommendationModelInfo {
	private static final long serialVersionUID = -7248721538070702182L;
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

	public AlsModelInfo() {
	}

	public AlsModelInfo(int userNum, int itemNum, int totalSamples, Params meta) {
		this.userNum = userNum;
		this.itemNum = itemNum;
		this.totalSamples = totalSamples;
		this.meta = meta;
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

	public int getNumFactors() {
		return meta.get(AlsTrainParams.RANK);
	}

	public int getNumIters() {
		return meta.get(AlsTrainParams.NUM_ITER);
	}

	public double getLambda() {
		return meta.get(AlsTrainParams.LAMBDA);
	}

	@Override
	public String toString() {

		StringBuilder sbd = new StringBuilder();
		sbd.append(modelSummaryHead("ALS"));

		Map <String, Object> config = new HashMap <>();
		config.put("NumFactors", getNumFactors());
		config.put("NumIters", getNumIters());
		config.put("Lambda", getLambda());

		sbd.append(PrettyDisplayUtils.displayHeadline("Train Parameters", '-'));
		sbd.append(PrettyDisplayUtils.displayMap(config, 5, true));
		return sbd.toString();
	}
}
