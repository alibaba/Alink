package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.recommendation.AlsRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;

/**
 * This op recommend users for item with als model.
 */
public class AlsUsersPerItemRecommBatchOp
	extends BaseRecommBatchOp <AlsUsersPerItemRecommBatchOp>
	implements BaseUsersPerItemRecommParams <AlsUsersPerItemRecommBatchOp> {

	private static final long serialVersionUID = 5935012361872088528L;

	public AlsUsersPerItemRecommBatchOp() {
		this(null);
	}

	public AlsUsersPerItemRecommBatchOp(Params params) {
		super(AlsRecommKernel::new, RecommType.USERS_PER_ITEM, params);
	}
}
