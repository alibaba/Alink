package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToKvParams;

/**
 * Transform data type from Json to Kv.
 */
@NameCn("JSON转KV")
public class JsonToKv extends BaseFormatTrans <JsonToKv> implements JsonToKvParams <JsonToKv> {

	private static final long serialVersionUID = 7081758123529110257L;

	public JsonToKv() {
		this(new Params());
	}

	public JsonToKv(Params params) {
		super(FormatType.JSON, FormatType.KV, params);
	}
}

