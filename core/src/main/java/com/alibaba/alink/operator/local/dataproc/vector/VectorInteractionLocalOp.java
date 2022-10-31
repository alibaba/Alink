package com.alibaba.alink.operator.local.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.vector.VectorInteractionMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.dataproc.vector.VectorInteractionParams;

/**
 * VectorInteraction is a Transformer which takes vector or double-valued columns, and generates a single vector column
 * that contains the product of all combinations of one value from each input column.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量元素两两相乘")
public final class VectorInteractionLocalOp extends MapLocalOp <VectorInteractionLocalOp>
	implements VectorInteractionParams <VectorInteractionLocalOp> {

	public VectorInteractionLocalOp() {
		this(null);
	}

	public VectorInteractionLocalOp(Params params) {
		super(VectorInteractionMapper::new, params);
	}
}
