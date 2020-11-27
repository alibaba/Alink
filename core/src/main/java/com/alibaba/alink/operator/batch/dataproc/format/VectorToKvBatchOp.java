package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToKvParams;

/**
 * Transform data type from Vector to Kv.
 */
public class VectorToKvBatchOp extends BaseFormatTransBatchOp <VectorToKvBatchOp>
	implements VectorToKvParams <VectorToKvBatchOp> {

	private static final long serialVersionUID = -2662940070674786484L;

	public VectorToKvBatchOp() {
		this(new Params());
	}

	public VectorToKvBatchOp(Params params) {
		super(FormatType.VECTOR, FormatType.KV, params);
	}
}
