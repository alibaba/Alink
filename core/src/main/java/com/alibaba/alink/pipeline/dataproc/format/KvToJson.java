package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToJsonParams;

/**
 * Transform data type from Kv to Json.
 */
@NameCn("KV转JSON")
public class KvToJson extends BaseFormatTrans <KvToJson> implements KvToJsonParams <KvToJson> {

	private static final long serialVersionUID = -5797501086945092198L;

	public KvToJson() {
		this(new Params());
	}

	public KvToJson(Params params) {
		super(FormatType.KV, FormatType.JSON, params);
	}
}

