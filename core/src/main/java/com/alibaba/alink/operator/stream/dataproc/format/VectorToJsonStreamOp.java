package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToJsonParams;

/**
 * Transform data type from Vector to Json.
 */
public class VectorToJsonStreamOp extends BaseFormatTransStreamOp <VectorToJsonStreamOp>
	implements VectorToJsonParams <VectorToJsonStreamOp> {

	private static final long serialVersionUID = -5444618549055672519L;

	public VectorToJsonStreamOp() {
		this(new Params());
	}

	public VectorToJsonStreamOp(Params params) {
		super(FormatType.VECTOR, FormatType.JSON, params);
	}
}
