package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.AlsRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseSimilarUsersRecommParams;

/**
 * Recommend similar items for the given item in stream format.
 */
public class AlsSimilarUsersRecommStreamOp extends BaseRecommStreamOp <AlsSimilarUsersRecommStreamOp>
	implements BaseSimilarUsersRecommParams <AlsSimilarUsersRecommStreamOp> {

	private static final long serialVersionUID = 8709408965733146312L;

	public AlsSimilarUsersRecommStreamOp(BatchOperator <?> model) {
		this(model, null);
	}

	public AlsSimilarUsersRecommStreamOp(BatchOperator <?> model, Params params) {
		super(model, AlsRecommKernel::new, RecommType.SIMILAR_USERS, params);
	}
}
