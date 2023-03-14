package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.common.recommendation.UserCfRecommKernel;
import com.alibaba.alink.params.recommendation.BaseSimilarUsersRecommParams;

/**
 * Recommend similar items for the given item.
 */
@NameCn("UserCf：相似users推荐")
@NameEn("UserCf Similar Users Recommend")
public class UserCfSimilarUsersRecommBatchOp extends BaseRecommBatchOp <UserCfSimilarUsersRecommBatchOp>
	implements BaseSimilarUsersRecommParams <UserCfSimilarUsersRecommBatchOp> {

	private static final long serialVersionUID = -1376148749814146249L;

	public UserCfSimilarUsersRecommBatchOp() {
		this(null);
	}

	public UserCfSimilarUsersRecommBatchOp(Params params) {
		super(UserCfRecommKernel::new, RecommType.SIMILAR_USERS, params);
	}
}
