package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.recommendation.AlsRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;

/**
 * This op recommend items for user with als model.
 */
public class AlsItemsPerUserRecommBatchOp
	extends BaseRecommBatchOp <AlsItemsPerUserRecommBatchOp>
	implements BaseItemsPerUserRecommParams <AlsItemsPerUserRecommBatchOp> {

	private static final long serialVersionUID = -2150705676921634637L;

	public AlsItemsPerUserRecommBatchOp() {
		this(null);
	}

	public AlsItemsPerUserRecommBatchOp(Params params) {
		super(AlsRecommKernel::new, RecommType.ITEMS_PER_USER, params);
	}
}
