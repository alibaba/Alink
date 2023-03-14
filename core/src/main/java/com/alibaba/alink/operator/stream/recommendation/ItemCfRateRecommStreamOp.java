package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.ItemCfRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseRateRecommParams;

/**
 * Rating user-item pair with itemCF model.
 */
@NameEn("ItemCf：Rate Recommendation")
@NameCn("ItemCf：打分推荐")
public class ItemCfRateRecommStreamOp
	extends BaseRecommStreamOp <ItemCfRateRecommStreamOp>
	implements BaseRateRecommParams <ItemCfRateRecommStreamOp> {

	private static final long serialVersionUID = -8996238236441477195L;

	public ItemCfRateRecommStreamOp(BatchOperator <?> model) {
		this(model, null);
	}

	public ItemCfRateRecommStreamOp(BatchOperator <?> model, Params params) {
		super(model, ItemCfRecommKernel::new, RecommType.RATE, params);
	}
}
