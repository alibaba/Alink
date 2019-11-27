package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorToColumnsMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorToColumnsParams;

/**
 * Transform vector to table columns. this class maps vector column to many columns.
 *
 */
public final class VectorToColumnsStreamOp extends MapStreamOp <VectorToColumnsStreamOp>
	implements VectorToColumnsParams <VectorToColumnsStreamOp> {

	public VectorToColumnsStreamOp() {
		this(null);
	}

	public VectorToColumnsStreamOp(Params params) {
		super(VectorToColumnsMapper::new, params);
	}

}
