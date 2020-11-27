package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.StringIndexerModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.StringIndexerPredictParams;

/**
 * Map string to index.
 */
public final class StringIndexerPredictStreamOp
	extends ModelMapStreamOp <StringIndexerPredictStreamOp>
	implements StringIndexerPredictParams <StringIndexerPredictStreamOp> {

	private static final long serialVersionUID = -6599742412462261688L;

	public StringIndexerPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public StringIndexerPredictStreamOp(BatchOperator model, Params params) {
		super(model, StringIndexerModelMapper::new, params);
	}
}
