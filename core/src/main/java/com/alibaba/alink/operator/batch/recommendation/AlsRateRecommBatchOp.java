package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.recommendation.AlsRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseRateRecommParams;

/**
 * this op rating user item pair with als model.
 */
@NameCn("ALS：打分推荐推荐")
public class AlsRateRecommBatchOp
	extends BaseRecommBatchOp <AlsRateRecommBatchOp>
	implements BaseRateRecommParams <AlsRateRecommBatchOp> {

	private static final long serialVersionUID = -9090784098056768786L;

	public AlsRateRecommBatchOp() {
		this(null);
	}

	public AlsRateRecommBatchOp(Params params) {
		super(AlsRecommKernel::new, RecommType.RATE, params);
	}
}
