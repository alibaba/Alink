package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToKvParams;

/**
 * Transform data type from Vector to Kv.
 */
public class VectorToKvStreamOp extends BaseFormatTransStreamOp <VectorToKvStreamOp>
	implements VectorToKvParams <VectorToKvStreamOp> {

	private static final long serialVersionUID = -2489632566101209556L;

	public VectorToKvStreamOp() {
		this(new Params());
	}

	public VectorToKvStreamOp(Params params) {
		super(FormatType.VECTOR, FormatType.KV, params);
	}
}
