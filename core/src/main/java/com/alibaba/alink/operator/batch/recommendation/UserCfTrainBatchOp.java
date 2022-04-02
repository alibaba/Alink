package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.UserCfModelInfo;
import com.alibaba.alink.params.recommendation.UserCfRecommTrainParams;

/**
 * A model that ranks an item according to its calc to other items observed for the user in question.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = {
	@PortSpec(PortType.MODEL)
})
@ParamSelectColumnSpec(name = "userCol")
@ParamSelectColumnSpec(name = "itemCol")
@ParamSelectColumnSpec(name = "rateCol",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("UserCf训练")
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
