package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * MultiLabel classification evaluation metrics.
 */
public class RankingMetrics extends BaseSimpleMultiLabelMetrics <RankingMetrics> {
	private static final long serialVersionUID = 3357268529296753541L;

	static final ParamInfo <Double> HIT_RATE = ParamInfoFactory
		.createParamInfo("hitRate", Double.class)
		.setDescription("hit rate")
		.setRequired()
		.build();

	static final ParamInfo <Double> AVE_RECIPRO_HIT_RANK = ParamInfoFactory
		.createParamInfo("averageReciprocalHitRank", Double.class)
		.setDescription("Average Reciprocal Hit Rank")
		.setRequired()
		.build();

	static final ParamInfo <double[]> PRECISION_ARRAY = ParamInfoFactory
		.createParamInfo("precisionArray", double[].class)
		.setDescription("precision list, PRECISION: TP / (TP + FP)")
		.setRequired()
		.build();

	static final ParamInfo <double[]> RECALL_ARRAY = ParamInfoFactory
		.createParamInfo("RecallArray", double[].class)
		.setDescription("recall list, recall == TPR")
		.setRequired()
		.build();

	static final ParamInfo <Double> MAP = ParamInfoFactory
		.createParamInfo("map", Double.class)
		.setDescription("map")
		.setRequired()
		.build();

	static final ParamInfo <double[]> NDCG_ARRAY = ParamInfoFactory
		.createParamInfo("ndcgArray", double[].class)
		.setDescription("ndcg")
		.setRequired()
		.build();

	public RankingMetrics(Row row) {
		super(row);
	}

	public RankingMetrics(Params params) {
		super(params);
	}

	public double getNdcg(int k) {
		double[] ndcgArray = getParams().get(NDCG_ARRAY);
		if (k - 1 >= ndcgArray.length) {
			return ndcgArray[ndcgArray.length - 1];
		} else {
			return ndcgArray[k - 1];
		}
	}

	public double getMap() {
		return get(MAP);
	}

	public double getPrecisionAtK(int k) {
		double[] precisionArray = getParams().get(PRECISION_ARRAY);
		if (k - 1 >= precisionArray.length) {
			return precisionArray[precisionArray.length - 1] * (precisionArray.length) / k;
		} else {
			return precisionArray[k - 1];
		}
	}

	public double getHitRate() {
		return get(HIT_RATE);
	}

	public double getArHr() {
		return get(AVE_RECIPRO_HIT_RANK);
	}

	public double getRecallAtK(int k) {
		double[] recallArray = getParams().get(RECALL_ARRAY);
		if (k - 1 >= recallArray.length) {
			return recallArray[recallArray.length - 1];
		} else {
			return recallArray[k - 1];
		}
	}

}
