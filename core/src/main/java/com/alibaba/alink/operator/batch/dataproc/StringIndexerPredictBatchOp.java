package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.StringIndexerModelMapper;
import com.alibaba.alink.params.dataproc.StringIndexerPredictParams;

/**
 * Map string to index.
 */
public final class StringIndexerPredictBatchOp
	extends ModelMapBatchOp <StringIndexerPredictBatchOp>
	implements StringIndexerPredictParams <StringIndexerPredictBatchOp> {

	private static final long serialVersionUID = 3074096923032622056L;

	public StringIndexerPredictBatchOp() {
		this(new Params());
	}

	public StringIndexerPredictBatchOp(Params params) {
		super(StringIndexerModelMapper::new, params);
	}
}
