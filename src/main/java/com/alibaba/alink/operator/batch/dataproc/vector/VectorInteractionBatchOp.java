package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorInteractionMapper;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.params.dataproc.vector.VectorInteractionParams;

/**
 * VectorInteraction is a Transformer which takes vector or double-valued columns, and generates a single vector column
 * that contains the product of all combinations of one value from each input column.
 */
public final class VectorInteractionBatchOp extends MapBatchOp <VectorInteractionBatchOp>
	implements VectorInteractionParams <VectorInteractionBatchOp> {

	public VectorInteractionBatchOp() {
		this(null);
	}

	public VectorInteractionBatchOp(Params params) {
		super(VectorInteractionMapper::new, params);
	}
}
