package com.alibaba.alink.operator.stream.finance;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.finance.ScorecardModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.finance.ScorecardPredictParams;

@NameCn("评分卡预测")
@NameEn("Scorecard Prediction")
public class ScorecardPredictStreamOp extends ModelMapStreamOp <ScorecardPredictStreamOp>
	implements ScorecardPredictParams <ScorecardPredictStreamOp> {

	private static final long serialVersionUID = -765051327621189460L;

	public ScorecardPredictStreamOp() {
		super(ScorecardModelMapper::new, new Params());
	}

	public ScorecardPredictStreamOp(Params params) {
		super(ScorecardModelMapper::new, params);
	}

	public ScorecardPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public ScorecardPredictStreamOp(BatchOperator model, Params params) {
		super(model, ScorecardModelMapper::new, params);
	}
}
