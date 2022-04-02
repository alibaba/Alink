package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.AlsRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseRateRecommParams;

/**
 * this of rating user item pair with als model in stream format.
 */
@NameCn("ALS：打分推荐")
public class AlsRateRecommStreamOp
	extends BaseRecommStreamOp <AlsRateRecommStreamOp>
	implements BaseRateRecommParams <AlsRateRecommStreamOp> {

	private static final long serialVersionUID = 3930977882909580288L;

	public AlsRateRecommStreamOp(BatchOperator <?> model) {
		this(model, null);
	}

	public AlsRateRecommStreamOp(BatchOperator <?> model, Params params) {
		super(model, AlsRecommKernel::new, RecommType.RATE, params);
	}
}
