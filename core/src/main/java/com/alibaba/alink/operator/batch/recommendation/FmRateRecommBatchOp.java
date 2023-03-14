package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.recommendation.FmRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseRateRecommParams;

/**
 * Fm rating batch op for recommendation.
 */
@NameCn("FM：打分推荐")
@NameEn("FM：Rate Recommend")
public class FmRateRecommBatchOp
	extends BaseRecommBatchOp <FmRateRecommBatchOp>
	implements BaseRateRecommParams <FmRateRecommBatchOp> {

	private static final long serialVersionUID = 1319094095294795555L;

	public FmRateRecommBatchOp() {
		this(null);
	}

	public FmRateRecommBatchOp(Params params) {
		super(FmRecommKernel::new, RecommType.RATE, params);
	}
}
