package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.recommendation.AlsRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseSimilarItemsRecommParams;

/**
 * Recommend similar items for the given item.
 */
@NameCn("ALS：相似items推荐")
public class AlsSimilarItemsRecommBatchOp extends BaseRecommBatchOp <AlsSimilarItemsRecommBatchOp>
	implements BaseSimilarItemsRecommParams <AlsSimilarItemsRecommBatchOp> {

	private static final long serialVersionUID = -8998130626286402739L;

	public AlsSimilarItemsRecommBatchOp() {
		this(null);
	}

	public AlsSimilarItemsRecommBatchOp(Params params) {
		super(AlsRecommKernel::new, RecommType.SIMILAR_ITEMS, params);
	}
}
