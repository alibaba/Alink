package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.UserCfModelInfo;
import com.alibaba.alink.params.recommendation.UserCfRecommTrainParams;

/**
 * A model that ranks an item according to its calc to other items observed for the user in question.
 */
public class UserCfTrainBatchOp extends BatchOperator <UserCfTrainBatchOp>
	implements UserCfRecommTrainParams <UserCfTrainBatchOp>,
	WithModelInfoBatchOp <UserCfModelInfo, UserCfTrainBatchOp, UserCfModelInfoBatchOp> {

	private static final long serialVersionUID = 254598990292465649L;

	public UserCfTrainBatchOp() {
		super(new Params());
	}

	public UserCfTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public UserCfTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		ItemCfTrainBatchOp op = new ItemCfTrainBatchOp(this.getParams())
			.setUserCol(this.getItemCol())
			.setItemCol(this.getUserCol())
			.linkFrom(inputs);
		this.setOutput(op.getDataSet(), op.getSchema());
		return this;
	}

	@Override
	public UserCfModelInfoBatchOp getModelInfoBatchOp() {
		return new UserCfModelInfoBatchOp(this.getParams()).linkFrom(this);
	}
}
