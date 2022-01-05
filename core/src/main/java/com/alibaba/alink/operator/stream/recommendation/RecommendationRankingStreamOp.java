package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.RecommendationRankingMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.recommendation.RecommendationRankingParams;

public class RecommendationRankingStreamOp
	extends ModelMapStreamOp <RecommendationRankingStreamOp>
	implements RecommendationRankingParams <RecommendationRankingStreamOp> {

	private static final long serialVersionUID = 2765835687511581198L;

	public RecommendationRankingStreamOp(BatchOperator<?> model) {
		this(model,null);
	}

	public RecommendationRankingStreamOp(BatchOperator<?> model, Params params) {
		super(model, RecommendationRankingMapper::new, params);
	}
}
