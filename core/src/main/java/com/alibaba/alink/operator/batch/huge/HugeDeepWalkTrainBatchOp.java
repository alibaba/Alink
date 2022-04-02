package com.alibaba.alink.operator.batch.huge;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.huge.impl.DeepWalkImpl;
import com.alibaba.alink.operator.common.aps.ApsCheckpoint;
import com.alibaba.alink.params.huge.HasNumCheckpoint;

@NameCn("大规模DeepWalk")
public final class HugeDeepWalkTrainBatchOp extends DeepWalkImpl <HugeDeepWalkTrainBatchOp>
	implements HasNumCheckpoint <HugeDeepWalkTrainBatchOp> {
	private static final long serialVersionUID = 5413242732809242754L;

	public HugeDeepWalkTrainBatchOp() {
		this(new Params());
	}

	public HugeDeepWalkTrainBatchOp(Params params) {
		this(params, null);
	}

	public HugeDeepWalkTrainBatchOp(ApsCheckpoint checkpoint) {
		this(null, checkpoint);
	}

	public HugeDeepWalkTrainBatchOp(Params params, ApsCheckpoint checkpoint) {
		super(params, checkpoint);
	}

}
