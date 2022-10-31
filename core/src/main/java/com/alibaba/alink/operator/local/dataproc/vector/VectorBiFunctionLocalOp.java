package com.alibaba.alink.operator.local.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.vector.VectorBiFunctionMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.dataproc.vector.VectorBiFunctionParams;

/**
 * Vector operator with two input vectors.
 * Vector can be sparse vector or dense vector.
 */
@NameCn("二元向量函数")
public final class VectorBiFunctionLocalOp extends MapLocalOp <VectorBiFunctionLocalOp>
	implements VectorBiFunctionParams <VectorBiFunctionLocalOp> {

	public VectorBiFunctionLocalOp() {
		this(null);
	}

	public VectorBiFunctionLocalOp(Params params) {
		super(VectorBiFunctionMapper::new, params);
	}
}
