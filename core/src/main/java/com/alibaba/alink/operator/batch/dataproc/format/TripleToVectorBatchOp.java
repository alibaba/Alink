package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.TripleToVectorParams;

/**
 * Transform data type from Triple to Vector.
 */
public class TripleToVectorBatchOp extends TripleToAnyBatchOp <TripleToVectorBatchOp>
	implements TripleToVectorParams <TripleToVectorBatchOp> {

	private static final long serialVersionUID = 1983797409096225871L;

	public TripleToVectorBatchOp() {
		this(new Params());
	}

	public TripleToVectorBatchOp(Params params) {
		super(FormatType.VECTOR, params);
	}
}
