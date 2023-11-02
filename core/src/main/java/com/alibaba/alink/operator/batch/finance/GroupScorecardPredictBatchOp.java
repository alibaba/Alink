package com.alibaba.alink.operator.batch.finance;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.finance.group.GroupScorecardModelMapper;
import com.alibaba.alink.params.finance.ScorecardPredictParams;

@NameCn("分群评分卡预测")
@NameEn("Group Score Predict")
public class GroupScorecardPredictBatchOp extends ModelMapBatchOp <GroupScorecardPredictBatchOp>
	implements ScorecardPredictParams <GroupScorecardPredictBatchOp> {

	public GroupScorecardPredictBatchOp() {
		this(new Params());
	}

	public GroupScorecardPredictBatchOp(Params params) {
		super(GroupScorecardModelMapper::new, params);
	}
}
