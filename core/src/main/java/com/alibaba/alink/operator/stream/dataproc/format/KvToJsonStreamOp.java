package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToJsonParams;

/**
 * Transform data type from Kv to Json.
 */
public class KvToJsonStreamOp extends BaseFormatTransStreamOp <KvToJsonStreamOp>
	implements KvToJsonParams <KvToJsonStreamOp> {

	private static final long serialVersionUID = 5167041026296967441L;

	public KvToJsonStreamOp() {
		this(new Params());
	}

	public KvToJsonStreamOp(Params params) {
		super(FormatType.KV, FormatType.JSON, params);
	}
}
