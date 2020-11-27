package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToCsvParams;

/**
 * Transform data type from Vector to Csv.
 */
public class VectorToCsvBatchOp extends BaseFormatTransBatchOp <VectorToCsvBatchOp>
	implements VectorToCsvParams <VectorToCsvBatchOp> {

	private static final long serialVersionUID = -5841372304077179838L;

	public VectorToCsvBatchOp() {
		this(new Params());
	}

	public VectorToCsvBatchOp(Params params) {
		super(FormatType.VECTOR, FormatType.CSV, params);
	}
}
