package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.recommendation.RecommendationRankingMapper;
import com.alibaba.alink.params.recommendation.RecommendationRankingParams;

public class RecommendationRankingBatchOp
	extends ModelMapBatchOp <RecommendationRankingBatchOp>
	implements RecommendationRankingParams <RecommendationRankingBatchOp> {

	private static final long serialVersionUID = 2765835687511581198L;

	public RecommendationRankingBatchOp() {
		this(null);
	}

	public RecommendationRankingBatchOp(Params params) {
		super(RecommendationRankingMapper::new, params);
	}
}
