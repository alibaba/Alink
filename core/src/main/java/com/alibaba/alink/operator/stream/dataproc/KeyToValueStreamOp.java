package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.KeyToValueModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.KeyToValueParams;

/**
 * stream op for key to value.
 */
public class KeyToValueStreamOp extends ModelMapStreamOp <KeyToValueStreamOp>
	implements KeyToValueParams <KeyToValueStreamOp> {

	private static final long serialVersionUID = 4638462768236553636L;

	public KeyToValueStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public KeyToValueStreamOp(BatchOperator model, Params params) {
		super(model, KeyToValueModelMapper::new, params);
	}

}
