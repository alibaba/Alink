package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.recommendation.AlsRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseSimilarUsersRecommParams;

/**
 * Recommend similar users for the given item.
 */
@NameCn("ALS：相似users推荐")
public class AlsSimilarUsersRecommender
	extends BaseRecommender <AlsSimilarUsersRecommender>
	implements BaseSimilarUsersRecommParams<AlsSimilarUsersRecommender> {

	private static final long serialVersionUID = -950826094014887702L;

	public AlsSimilarUsersRecommender() {
		this(null);
	}

	public AlsSimilarUsersRecommender(Params params) {
		super(AlsRecommKernel::new, RecommType.SIMILAR_USERS, params);
	}
}
