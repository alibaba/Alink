package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.recommendation.FmRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseRateRecommParams;

/**
 * Fm recommendation pipeline for rating.
 */
@NameCn("FM：打分推荐")
public class FmRateRecommender
	extends BaseRecommender <FmRateRecommender>
	implements BaseRateRecommParams <FmRateRecommender> {

	private static final long serialVersionUID = 7321622751763845452L;

	public FmRateRecommender() {
		this(null);
	}

	public FmRateRecommender(Params params) {
		super(FmRecommKernel::new, RecommType.RATE, params);
	}
}
