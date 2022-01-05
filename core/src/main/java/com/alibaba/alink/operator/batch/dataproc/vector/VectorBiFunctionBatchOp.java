package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorBiFunctionMapper;
import com.alibaba.alink.params.dataproc.vector.VectorBiFunctionParams;

/**
 * Vector operator with two input vectors.
 * Vector can be sparse vector or dense vector.
 */
public final class VectorBiFunctionBatchOp extends MapBatchOp <VectorBiFunctionBatchOp>
	implements VectorBiFunctionParams <VectorBiFunctionBatchOp> {

	private static final long serialVersionUID = -5580521679568956131L;

	public VectorBiFunctionBatchOp() {
		this(null);
	}

	public VectorBiFunctionBatchOp(Params params) {
		super(VectorBiFunctionMapper::new, params);
	}
}
