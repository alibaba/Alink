package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.recommendation.AlsRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseSimilarUsersRecommParams;

/**
 * Recommend similar items for the given item.
 */
@NameCn("ALS：相似users推荐")
@NameEn("ALS Similar Users Recommend")
public class AlsSimilarUsersRecommBatchOp extends BaseRecommBatchOp <AlsSimilarUsersRecommBatchOp>
	implements BaseSimilarUsersRecommParams <AlsSimilarUsersRecommBatchOp> {

	private static final long serialVersionUID = 2281617437111679056L;

	public AlsSimilarUsersRecommBatchOp() {
		this(null);
	}

	public AlsSimilarUsersRecommBatchOp(Params params) {
		super(AlsRecommKernel::new, RecommType.SIMILAR_USERS, params);
	}
}
