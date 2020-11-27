package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToKvParams;

/**
 * Transform data type from Json to Kv.
 */
public class JsonToKvStreamOp extends BaseFormatTransStreamOp <JsonToKvStreamOp>
	implements JsonToKvParams <JsonToKvStreamOp> {

	private static final long serialVersionUID = 5236567848899059440L;

	public JsonToKvStreamOp() {
		this(new Params());
	}

	public JsonToKvStreamOp(Params params) {
		super(FormatType.JSON, FormatType.KV, params);
	}
}
