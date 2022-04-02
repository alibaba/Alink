package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.recommendation.FmRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;

/**
 * Fm recommendation pipeline for recommending items to user.
 */
@NameCn("FM：ItemsPerUser推荐")
public class FmItemsPerUserRecommender
	extends BaseRecommender <FmItemsPerUserRecommender>
	implements BaseItemsPerUserRecommParams <FmItemsPerUserRecommender> {

	private static final long serialVersionUID = 6862957471671534918L;

	public FmItemsPerUserRecommender() {
		this(null);
	}

	public FmItemsPerUserRecommender(Params params) {
		super(FmRecommKernel::new, RecommType.ITEMS_PER_USER, params);
	}
}
