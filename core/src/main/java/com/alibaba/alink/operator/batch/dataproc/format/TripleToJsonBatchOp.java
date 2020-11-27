package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.TripleToJsonParams;

/**
 * Transform data type from Triple to Json.
 */
public class TripleToJsonBatchOp extends TripleToAnyBatchOp <TripleToJsonBatchOp>
	implements TripleToJsonParams <TripleToJsonBatchOp> {

	private static final long serialVersionUID = 8526120093675323151L;

	public TripleToJsonBatchOp() {
		this(new Params());
	}

	public TripleToJsonBatchOp(Params params) {
		super(FormatType.JSON, params);
	}
}
