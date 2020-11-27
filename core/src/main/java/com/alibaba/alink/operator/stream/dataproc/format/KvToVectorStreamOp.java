package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToVectorParams;

/**
 * Transform data type from Kv to Vector.
 */
public class KvToVectorStreamOp extends BaseFormatTransStreamOp <KvToVectorStreamOp>
	implements KvToVectorParams <KvToVectorStreamOp> {

	private static final long serialVersionUID = -1890178421698511469L;

	public KvToVectorStreamOp() {
		this(new Params());
	}

	public KvToVectorStreamOp(Params params) {
		super(FormatType.KV, FormatType.VECTOR, params);
	}
}
