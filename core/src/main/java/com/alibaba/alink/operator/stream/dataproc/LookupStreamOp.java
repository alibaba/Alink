package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.LookupModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.LookupParams;

/**
 * stream op for lookup.
 */
public class LookupStreamOp extends ModelMapStreamOp <LookupStreamOp>
	implements LookupParams<LookupStreamOp> {

	public LookupStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public LookupStreamOp(BatchOperator model, Params params) {
		super(model, LookupModelMapper::new, params);
	}

}
