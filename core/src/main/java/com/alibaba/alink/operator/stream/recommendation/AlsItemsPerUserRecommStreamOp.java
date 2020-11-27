package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.AlsRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;

/**
 * This op recommend items for user with als model in stream format.
 */
public class AlsItemsPerUserRecommStreamOp
	extends BaseRecommStreamOp <AlsItemsPerUserRecommStreamOp>
	implements BaseItemsPerUserRecommParams <AlsItemsPerUserRecommStreamOp> {

	private static final long serialVersionUID = -4991595915487178731L;

	public AlsItemsPerUserRecommStreamOp(BatchOperator model) {
		this(model, null);
	}

	public AlsItemsPerUserRecommStreamOp(BatchOperator model, Params params) {
		super(model, AlsRecommKernel::new, RecommType.ITEMS_PER_USER, params);
	}
}
