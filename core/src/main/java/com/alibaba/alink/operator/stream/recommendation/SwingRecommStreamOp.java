package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.common.recommendation.SwingRecommKernel;
import com.alibaba.alink.params.recommendation.BaseSimilarItemsRecommParams;

@NameCn("swing推荐")
public class SwingRecommStreamOp extends BaseRecommStreamOp <SwingRecommStreamOp>
	implements BaseSimilarItemsRecommParams <SwingRecommStreamOp> {
	public SwingRecommStreamOp(BatchOperator <?> model) {
		this(model, null);
	}

	public SwingRecommStreamOp(BatchOperator <?> model, Params params) {
		super(model, SwingRecommKernel::new, RecommType.SIMILAR_ITEMS, params);
	}
}
