package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.recommendation.AlsRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;

/**
 * This pipeline recommend items for user with als model.
 */
@NameCn("ALS：ItemsPerUser推荐")
public class AlsItemsPerUserRecommender
	extends BaseRecommender <AlsItemsPerUserRecommender>
	implements BaseItemsPerUserRecommParams <AlsItemsPerUserRecommender> {

	private static final long serialVersionUID = 2142224567350244552L;

	public AlsItemsPerUserRecommender() {
		this(null);
	}

	public AlsItemsPerUserRecommender(Params params) {
		super(AlsRecommKernel::new, RecommType.ITEMS_PER_USER, params);
	}
}
