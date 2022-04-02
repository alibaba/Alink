package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.common.recommendation.UserCfRecommKernel;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;

/**
 * Recommend similar items for the given item.
 */
@NameCn("UserCf：ItemsPerUser推荐")
public class UserCfItemsPerUserRecommBatchOp extends BaseRecommBatchOp <UserCfItemsPerUserRecommBatchOp>
	implements BaseItemsPerUserRecommParams <UserCfItemsPerUserRecommBatchOp> {

	private static final long serialVersionUID = 5038760713397865607L;

	public UserCfItemsPerUserRecommBatchOp() {
		this(null);
	}

	public UserCfItemsPerUserRecommBatchOp(Params params) {
		super(UserCfRecommKernel::new, RecommType.ITEMS_PER_USER, params);
	}
}
