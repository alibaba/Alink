package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.AlsRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseSimilarItemsRecommParams;

/**
 * Recommend similar items for the given item in stream format.
 */
public class AlsSimilarItemsRecommStreamOp extends BaseRecommStreamOp <AlsSimilarItemsRecommStreamOp>
	implements BaseSimilarItemsRecommParams <AlsSimilarItemsRecommStreamOp> {

	private static final long serialVersionUID = -5005943967509093904L;

	public AlsSimilarItemsRecommStreamOp(BatchOperator model) {
		this(model, null);
	}

	public AlsSimilarItemsRecommStreamOp(BatchOperator model, Params params) {
		super(model, AlsRecommKernel::new, RecommType.SIMILAR_ITEMS, params);
	}
}
