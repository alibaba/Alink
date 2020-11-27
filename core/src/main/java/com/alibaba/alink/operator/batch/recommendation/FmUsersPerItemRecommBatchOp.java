package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.recommendation.FmRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;

/**
 * Fm recommendation batch op for recommending users to item.
 */
public class FmUsersPerItemRecommBatchOp
	extends BaseRecommBatchOp <FmUsersPerItemRecommBatchOp>
	implements BaseUsersPerItemRecommParams <FmUsersPerItemRecommBatchOp> {

	private static final long serialVersionUID = 8171071538483333422L;

	public FmUsersPerItemRecommBatchOp() {
		this(null);
	}

	public FmUsersPerItemRecommBatchOp(Params params) {
		super(FmRecommKernel::new, RecommType.USERS_PER_ITEM, params);
	}
}
