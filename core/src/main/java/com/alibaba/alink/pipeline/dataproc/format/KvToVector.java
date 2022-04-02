package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToVectorParams;

/**
 * Transform data type from Kv to Vector.
 */
@NameCn("KV转向量")
public class KvToVector extends BaseFormatTrans <KvToVector> implements KvToVectorParams <KvToVector> {

	private static final long serialVersionUID = -710863210407486659L;

	public KvToVector() {
		this(new Params());
	}

	public KvToVector(Params params) {
		super(FormatType.KV, FormatType.VECTOR, params);
	}
}

