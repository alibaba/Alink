package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.vector.VectorInteractionMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorInteractionParams;

/**
 * VectorInteraction is a Transformer which takes vector or double-valued columns, and generates a single vector column
 * that contains the product of all combinations of one value from each input column.
 */
@ParamSelectColumnSpec(name = "selectedCols", portIndices = 0, allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量元素两两相乘")
@NameEn("Vector interaction")
public final class VectorInteractionStreamOp extends MapStreamOp <VectorInteractionStreamOp>
	implements VectorInteractionParams <VectorInteractionStreamOp> {

	private static final long serialVersionUID = -6945452560346388467L;

	public VectorInteractionStreamOp(Params params) {
		super(VectorInteractionMapper::new, params);
	}

	public VectorInteractionStreamOp() {
		this(null);
	}
}
