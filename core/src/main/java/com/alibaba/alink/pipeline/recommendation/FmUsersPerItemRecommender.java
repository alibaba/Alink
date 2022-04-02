package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.recommendation.FmRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;

/**
 * Fm recommendation pipeline for recommending users to item.
 */
@NameCn("FM：UsersPerItem推荐")
public class FmUsersPerItemRecommender
	extends BaseRecommender <FmUsersPerItemRecommender>
	implements BaseUsersPerItemRecommParams <FmUsersPerItemRecommender> {

	private static final long serialVersionUID = -1427859937015411207L;

	public FmUsersPerItemRecommender() {
		this(null);
	}

	public FmUsersPerItemRecommender(Params params) {
		super(FmRecommKernel::new, RecommType.USERS_PER_ITEM, params);
	}
}
