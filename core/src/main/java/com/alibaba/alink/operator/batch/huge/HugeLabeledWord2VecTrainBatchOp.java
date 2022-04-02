package com.alibaba.alink.operator.batch.huge;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.huge.impl.LabeledWord2VecImpl;
import com.alibaba.alink.operator.common.aps.ApsCheckpoint;
import com.alibaba.alink.params.huge.HasNumCheckpoint;

@NameCn("大规模带标签的Word2Vec")
public final class HugeLabeledWord2VecTrainBatchOp extends LabeledWord2VecImpl <HugeLabeledWord2VecTrainBatchOp>
	implements HasNumCheckpoint <HugeLabeledWord2VecTrainBatchOp> {
	private static final long serialVersionUID = -3014286578422196705L;

	public HugeLabeledWord2VecTrainBatchOp() {
		this(new Params());
	}

	public HugeLabeledWord2VecTrainBatchOp(Params params) {
		this(params, null);
	}

	public HugeLabeledWord2VecTrainBatchOp(ApsCheckpoint checkpoint) {
		this(null, checkpoint);
	}

	public HugeLabeledWord2VecTrainBatchOp(Params params, ApsCheckpoint checkpoint) {
		super(params, checkpoint);
	}
}
