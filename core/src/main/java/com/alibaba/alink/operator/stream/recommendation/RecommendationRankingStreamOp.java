package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.RecommendationRankingMapper;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.recommendation.RecommendationRankingParams;
@ParamSelectColumnSpec(name = "mTableCol",
	allowedTypeCollections = TypeCollections.MTABLE_TYPES)
@NameCn("推荐组件：精排")
@NameEn("Recommendation Ranking")
public class RecommendationRankingStreamOp
	extends ModelMapStreamOp <RecommendationRankingStreamOp>
	implements RecommendationRankingParams <RecommendationRankingStreamOp> {

	private static final long serialVersionUID = 2765835687511581198L;

	public RecommendationRankingStreamOp() {
		super(RecommendationRankingMapper::new, new Params());
	}

	public RecommendationRankingStreamOp(Params params) {
		super(RecommendationRankingMapper::new, params);
	}

	public RecommendationRankingStreamOp(BatchOperator<?> model) {
		this(model,null);
	}

	public RecommendationRankingStreamOp(BatchOperator<?> model, Params params) {
		super(model, RecommendationRankingMapper::new, params);
	}
}
