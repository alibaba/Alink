package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.recommendation.RecommendationRankingMapper;
import com.alibaba.alink.params.recommendation.RecommendationRankingParams;
import com.alibaba.alink.pipeline.MapModel;
import com.alibaba.alink.pipeline.PipelineModel;

public class RecommendationRanking extends MapModel <RecommendationRanking>
	implements RecommendationRankingParams <RecommendationRanking> {

	public RecommendationRanking() {
		super(RecommendationRankingMapper::new, new Params());
	}

	public RecommendationRanking(Params params) {
		super(RecommendationRankingMapper::new, params);
	}

	public RecommendationRanking setPipelineModel(PipelineModel pipelineModel) {
		return super.setModelData(pipelineModel.save());
	}
}
