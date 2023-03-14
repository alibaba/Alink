package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToKvParams;

/**
 * Transform data type from Vector to Kv.
 */
@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量转KV")
@NameEn("Vector to key-value pairs")
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
