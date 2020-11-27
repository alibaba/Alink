package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToTripleParams;

/**
 * Transform data type from Json to Triple.
 */
public class JsonToTripleStreamOp extends AnyToTripleStreamOp <JsonToTripleStreamOp>
	implements JsonToTripleParams <JsonToTripleStreamOp> {

	private static final long serialVersionUID = 8653473208180902492L;

	public JsonToTripleStreamOp() {
		this(new Params());
	}

	public JsonToTripleStreamOp(Params params) {
		super(FormatType.JSON, params);
	}
}
