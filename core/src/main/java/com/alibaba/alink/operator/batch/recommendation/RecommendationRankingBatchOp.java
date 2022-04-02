package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.recommendation.RecommendationRankingMapper;
import com.alibaba.alink.params.recommendation.RecommendationRankingParams;
@ParamSelectColumnSpec(name = "mTableCol",
	allowedTypeCollections = TypeCollections.MTABLE_TYPES)
@NameCn("推荐组件：精排")
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
