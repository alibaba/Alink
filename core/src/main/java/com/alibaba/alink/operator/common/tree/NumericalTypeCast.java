package com.alibaba.alink.operator.common.tree;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.NumericalTypeCastMapper;
import com.alibaba.alink.params.dataproc.NumericalTypeCastParams;

/**
 * Cast the numerical columns to the specified type.
 *
 * <p>Internal operator.
 */
class NumericalTypeCast extends MapBatchOp <NumericalTypeCast> implements
	NumericalTypeCastParams <NumericalTypeCast> {
	private static final long serialVersionUID = 4336900852427563481L;

	public NumericalTypeCast() {
		this(new Params());
	}

	public NumericalTypeCast(Params params) {
		super(NumericalTypeCastMapper::new, params);
	}
}
