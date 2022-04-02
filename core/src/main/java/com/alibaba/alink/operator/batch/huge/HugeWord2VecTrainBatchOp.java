package com.alibaba.alink.operator.batch.huge;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.huge.impl.Word2VecImpl;
import com.alibaba.alink.operator.common.aps.ApsCheckpoint;
import com.alibaba.alink.params.huge.HasNumCheckpoint;

@NameCn("大规模Word2Vec")
public final class HugeWord2VecTrainBatchOp extends Word2VecImpl <HugeWord2VecTrainBatchOp>
	implements HasNumCheckpoint <HugeWord2VecTrainBatchOp> {
	private static final long serialVersionUID = -1222790480709681729L;

	public HugeWord2VecTrainBatchOp() {
		this(new Params());
	}

	public HugeWord2VecTrainBatchOp(Params params) {
		super(params);
	}

	public HugeWord2VecTrainBatchOp(ApsCheckpoint checkpoint) {
		super(null, checkpoint);
	}

	public HugeWord2VecTrainBatchOp(Params params, ApsCheckpoint checkpoint) {
		super(params, checkpoint);
	}
}
