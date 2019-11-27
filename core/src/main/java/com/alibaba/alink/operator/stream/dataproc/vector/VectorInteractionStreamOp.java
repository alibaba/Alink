package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorInteractionMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorInteractionParams;

/**
 * VectorInteraction is a Transformer which takes vector or double-valued columns, and generates a single vector column
 * that contains the product of all combinations of one value from each input column.
 *
 */
public final class VectorInteractionStreamOp extends MapStreamOp <VectorInteractionStreamOp>
	implements VectorInteractionParams <VectorInteractionStreamOp> {

	public VectorInteractionStreamOp(Params params) {
		super(VectorInteractionMapper::new, params);
	}

	public VectorInteractionStreamOp() {
		this(null);
	}
}
