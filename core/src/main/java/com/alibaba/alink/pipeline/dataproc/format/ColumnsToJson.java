package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToJsonParams;

/**
 * Transform data type from Columns to Json.
 */
public class ColumnsToJson extends BaseFormatTrans <ColumnsToJson> implements ColumnsToJsonParams <ColumnsToJson> {

	private static final long serialVersionUID = -4985219735588284091L;

	public ColumnsToJson() {
		this(new Params());
	}

	public ColumnsToJson(Params params) {
		super(FormatType.COLUMNS, FormatType.JSON, params);
	}
}

