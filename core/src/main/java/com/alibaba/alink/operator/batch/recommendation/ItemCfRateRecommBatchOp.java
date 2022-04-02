package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.recommendation.ItemCfRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseRateRecommParams;

/**
 * Rating for user-item pair with item CF model.
 */
@NameCn("ItemCf：打分推荐")
public class ItemCfRateRecommBatchOp
	extends BaseRecommBatchOp <ItemCfRateRecommBatchOp>
	implements BaseRateRecommParams <ItemCfRateRecommBatchOp> {

	private static final long serialVersionUID = 6828072772516197481L;

	public ItemCfRateRecommBatchOp() {
		this(null);
	}

	public ItemCfRateRecommBatchOp(Params params) {
		super(ItemCfRecommKernel::new, RecommType.RATE, params);
	}
}
