package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.AlsRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;

/**
 * This op recommend users for item with als model in stream format.
 */
public class AlsUsersPerItemRecommStreamOp
	extends BaseRecommStreamOp <AlsUsersPerItemRecommStreamOp>
	implements BaseUsersPerItemRecommParams <AlsUsersPerItemRecommStreamOp> {

	private static final long serialVersionUID = 4546856451984460529L;

	public AlsUsersPerItemRecommStreamOp(BatchOperator model) {
		this(model, null);
	}

	public AlsUsersPerItemRecommStreamOp(BatchOperator model, Params params) {
		super(model, AlsRecommKernel::new, RecommType.USERS_PER_ITEM, params);
	}
}
