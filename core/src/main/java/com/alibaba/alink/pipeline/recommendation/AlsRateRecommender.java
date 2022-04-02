package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.recommendation.AlsRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseRateRecommParams;

/**
 * this pipeline rating user item pair with als model.
 */
@NameCn("ALS：打分推荐")
public class AlsRateRecommender
	extends BaseRecommender <AlsRateRecommender>
	implements BaseRateRecommParams <AlsRateRecommender> {

	private static final long serialVersionUID = -8628335170578732542L;

	public AlsRateRecommender() {
		this(null);
	}

	public AlsRateRecommender(Params params) {
		super(AlsRecommKernel::new, RecommType.RATE, params);
	}
}
