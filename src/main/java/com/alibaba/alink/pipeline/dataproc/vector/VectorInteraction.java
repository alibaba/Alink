package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorInteractionMapper;
import com.alibaba.alink.params.dataproc.vector.VectorInteractionParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * VectorInteraction is a Transformer which takes vector or double-valued columns, and generates a single vector column
 * that contains the product of all combinations of one value from each input column.
 */
public class VectorInteraction extends MapTransformer<VectorInteraction>
	implements VectorInteractionParams <VectorInteraction> {

	public VectorInteraction() {
		this(null);
	}

	public VectorInteraction(Params params) {
		super(VectorInteractionMapper::new, params);
	}
}
