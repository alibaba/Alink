package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorFunctionMapper;
import com.alibaba.alink.params.dataproc.vector.VectorFunctionParams;

/**
 * Find maxValue / minValue / maxValue index / minValue index in Vector
 * Vector can be sparse vector or dense vector.
 */

public final class VectorFunctionBatchOp extends MapBatchOp <VectorFunctionBatchOp>
	implements VectorFunctionParams <VectorFunctionBatchOp> {

	private static final long serialVersionUID = -5580521679868956131L;

	public VectorFunctionBatchOp() {
		this(null);
	}

	public VectorFunctionBatchOp(Params params) {
		super(VectorFunctionMapper::new, params);
	}
}
