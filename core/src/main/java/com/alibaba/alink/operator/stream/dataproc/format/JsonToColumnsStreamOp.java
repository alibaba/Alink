package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToColumnsParams;

/**
 * Transform data type from Json to Columns.
 */
public class JsonToColumnsStreamOp extends BaseFormatTransStreamOp <JsonToColumnsStreamOp>
	implements JsonToColumnsParams <JsonToColumnsStreamOp> {

	private static final long serialVersionUID = 956660525115374945L;

	public JsonToColumnsStreamOp() {
		this(new Params());
	}

	public JsonToColumnsStreamOp(Params params) {
		super(FormatType.JSON, FormatType.COLUMNS, params);
	}
}
