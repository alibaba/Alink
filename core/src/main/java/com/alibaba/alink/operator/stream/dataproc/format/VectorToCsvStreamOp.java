package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToCsvParams;

/**
 * Transform data type from Vector to Csv.
 */
public class VectorToCsvStreamOp extends BaseFormatTransStreamOp <VectorToCsvStreamOp>
	implements VectorToCsvParams <VectorToCsvStreamOp> {

	private static final long serialVersionUID = 5373033053946898305L;

	public VectorToCsvStreamOp() {
		this(new Params());
	}

	public VectorToCsvStreamOp(Params params) {
		super(FormatType.VECTOR, FormatType.CSV, params);
	}
}
