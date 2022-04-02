package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToKvParams;

/**
 * Transform data type from Vector to Kv.
 */
@NameCn("向量转KV")
public class VectorToKv extends BaseFormatTrans <VectorToKv> implements VectorToKvParams <VectorToKv> {

	private static final long serialVersionUID = 5671363153811928426L;

	public VectorToKv() {
		this(new Params());
	}

	public VectorToKv(Params params) {
		super(FormatType.VECTOR, FormatType.KV, params);
	}
}

