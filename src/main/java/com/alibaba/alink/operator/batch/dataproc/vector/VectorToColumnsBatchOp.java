package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorToColumnsMapper;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.params.dataproc.vector.VectorToColumnsParams;

/**
 * Transform vector to table columns. This transformer will map vector column to columns as designed.
 */
public final class VectorToColumnsBatchOp extends MapBatchOp <VectorToColumnsBatchOp>
	implements VectorToColumnsParams <VectorToColumnsBatchOp> {

	public VectorToColumnsBatchOp() {
		this(null);
	}

	public VectorToColumnsBatchOp(Params params) {
		super(VectorToColumnsMapper::new, params);
	}
}
