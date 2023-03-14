package com.alibaba.alink.operator.batch.finance;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.finance.ScorecardModelMapper;
import com.alibaba.alink.params.finance.ScorecardPredictParams;

@NameCn("评分卡预测")
@NameEn("Score Predict")
public class ScorecardPredictBatchOp extends ModelMapBatchOp <ScorecardPredictBatchOp>
	implements ScorecardPredictParams <ScorecardPredictBatchOp> {

	private static final long serialVersionUID = -3498559932886873694L;

	public ScorecardPredictBatchOp() {
		this(new Params());
	}

	public ScorecardPredictBatchOp(Params params) {
		super(ScorecardModelMapper::new, params);
	}
}
