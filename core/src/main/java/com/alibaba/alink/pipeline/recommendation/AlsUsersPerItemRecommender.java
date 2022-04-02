package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.recommendation.AlsRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;

/**
 * This pipeline recommend users for item with als model.
 */
@NameCn("ALS：UsersPerItem推荐")
public class AlsUsersPerItemRecommender
	extends BaseRecommender <AlsUsersPerItemRecommender>
	implements BaseUsersPerItemRecommParams <AlsUsersPerItemRecommender> {

	private static final long serialVersionUID = 8046097577346323907L;

	public AlsUsersPerItemRecommender() {
		this(null);
	}

	public AlsUsersPerItemRecommender(Params params) {
		super(AlsRecommKernel::new, RecommType.USERS_PER_ITEM, params);
	}
}
