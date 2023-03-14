package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.vector.VectorFunctionMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorFunctionParams;

/**
 * Find maxValue / minValue / maxValue index / minValue index in Vector
 * Vector can be sparse vector or dense vector.
 */

@ParamSelectColumnSpec(name = "selectedCol", portIndices = 0, allowedTypeCollections = {TypeCollections.VECTOR_TYPES})
@NameCn("向量函数")
@NameEn("Vector function")
public final class VectorFunctionStreamOp extends MapStreamOp <VectorFunctionStreamOp>
	implements VectorFunctionParams <VectorFunctionStreamOp> {

	private static final long serialVersionUID = 6183736542103372491L;

	public VectorFunctionStreamOp() {
		this(null);
	}

	public VectorFunctionStreamOp(Params param) {
		super(VectorFunctionMapper::new, param);
	}

}
