package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorInteractionMapper;
import com.alibaba.alink.params.dataproc.vector.VectorInteractionParams;

/**
 * VectorInteraction is a Transformer which takes vector or double-valued columns, and generates a single vector column
 * that contains the product of all combinations of one value from each input column.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量元素两两相乘")
@NameEn("Vector Interaction")
public final class VectorInteractionBatchOp extends MapBatchOp <VectorInteractionBatchOp>
	implements VectorInteractionParams <VectorInteractionBatchOp> {

	private static final long serialVersionUID = -8345027445709445126L;

	public VectorInteractionBatchOp() {
		this(null);
	}

	public VectorInteractionBatchOp(Params params) {
		super(VectorInteractionMapper::new, params);
	}
}
