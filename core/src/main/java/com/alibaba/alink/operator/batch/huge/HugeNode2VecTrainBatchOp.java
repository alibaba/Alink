package com.alibaba.alink.operator.batch.huge;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.huge.impl.Node2VecImpl;
import com.alibaba.alink.operator.common.aps.ApsCheckpoint;
import com.alibaba.alink.params.huge.HasNumCheckpoint;

@NameCn("大规模Node2Vec")
@NameEn("Huge Node2Vec Training")
public final class HugeNode2VecTrainBatchOp extends Node2VecImpl <HugeNode2VecTrainBatchOp>
	implements HasNumCheckpoint <HugeNode2VecTrainBatchOp> {
	private static final long serialVersionUID = 4360078150555638432L;

	public HugeNode2VecTrainBatchOp() {
		this(new Params());
	}

	public HugeNode2VecTrainBatchOp(Params params) {
		this(params, null);
	}

	public HugeNode2VecTrainBatchOp(ApsCheckpoint checkpoint) {
		this(null, checkpoint);
	}

	public HugeNode2VecTrainBatchOp(Params params, ApsCheckpoint checkpoint) {
		super(params, checkpoint);
	}
}
