package com.alibaba.alink.operator.batch.huge;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.huge.impl.MetaPath2VecImpl;
import com.alibaba.alink.operator.common.aps.ApsCheckpoint;
import com.alibaba.alink.params.huge.HasNumCheckpoint;

@NameCn("大规模MethPath2Vec")
@NameEn("Huge Data MetaPath2Vec")
public final class HugeMetaPath2VecTrainBatchOp extends MetaPath2VecImpl <HugeMetaPath2VecTrainBatchOp>
	implements HasNumCheckpoint <HugeMetaPath2VecTrainBatchOp> {
	private static final long serialVersionUID = -8398787630956847264L;

	public HugeMetaPath2VecTrainBatchOp() {
		this(new Params());
	}

	public HugeMetaPath2VecTrainBatchOp(Params params) {
		this(params, null);
	}

	public HugeMetaPath2VecTrainBatchOp(ApsCheckpoint checkpoint) {
		this(null, checkpoint);
	}

	public HugeMetaPath2VecTrainBatchOp(Params params, ApsCheckpoint checkpoint) {
		super(params, checkpoint);
	}
}
