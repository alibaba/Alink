package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.recommendation.ItemCfRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseRateRecommParams;

/**
 * Rating for user-item pair with itemCF model.
 */
public class ItemCfRateRecommender
	extends BaseRecommender <ItemCfRateRecommender>
	implements BaseRateRecommParams <ItemCfRateRecommender> {

	private static final long serialVersionUID = 1747235774121801296L;

	public ItemCfRateRecommender() {
		this(null);
	}

	public ItemCfRateRecommender(Params params) {
		super(ItemCfRecommKernel::new, RecommType.RATE, params);
	}
}
