package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.recommendation.FmRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;

/**
 * Fm recommendation batch op for recommending items to user.
 */
public class FmItemsPerUserRecommBatchOp
	extends BaseRecommBatchOp <FmItemsPerUserRecommBatchOp>
	implements BaseItemsPerUserRecommParams <FmItemsPerUserRecommBatchOp> {

	private static final long serialVersionUID = -5659098927639038890L;

	public FmItemsPerUserRecommBatchOp() {
		this(null);
	}

	public FmItemsPerUserRecommBatchOp(Params params) {
		super(FmRecommKernel::new, RecommType.ITEMS_PER_USER, params);
	}
}
