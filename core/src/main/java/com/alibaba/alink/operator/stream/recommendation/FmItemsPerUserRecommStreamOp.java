package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.FmRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;

/**
 * Fm recommendation batch op for recommending items to user in stream format.
 */
@NameCn("FM：ItemsPerUser推荐")
public class FmItemsPerUserRecommStreamOp
	extends BaseRecommStreamOp <FmItemsPerUserRecommStreamOp>
	implements BaseItemsPerUserRecommParams <FmItemsPerUserRecommStreamOp> {

	private static final long serialVersionUID = -7133237507931279638L;

	public FmItemsPerUserRecommStreamOp(BatchOperator <?> model) {
		this(model, null);
	}

	public FmItemsPerUserRecommStreamOp(BatchOperator <?> model, Params params) {
		super(model, FmRecommKernel::new, RecommType.ITEMS_PER_USER, params);
	}
}
