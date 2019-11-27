package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.NumericalTypeCastMapper;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.params.dataproc.NumericalTypeCastParams;

/**
 * Cast the numerical columns to the specified type.
 *
 * <p>Internal operator.
 */
public class NumericalTypeCastBatchOp extends MapBatchOp <NumericalTypeCastBatchOp> implements
	NumericalTypeCastParams <NumericalTypeCastBatchOp> {
	public NumericalTypeCastBatchOp() {
		this(new Params());
	}

	public NumericalTypeCastBatchOp(Params params) {
		super(NumericalTypeCastMapper::new, params);
	}
}
