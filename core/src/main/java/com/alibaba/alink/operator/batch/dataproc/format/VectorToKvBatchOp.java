package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToKvParams;

/**
 * Transform data type from Vector to Kv.
 */
@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量转KV")
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
