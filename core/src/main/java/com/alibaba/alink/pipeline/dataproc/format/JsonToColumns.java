package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToColumnsParams;

/**
 * Transform data type from Json to Columns.
 */
public class JsonToColumns extends BaseFormatTrans <JsonToColumns> implements JsonToColumnsParams <JsonToColumns> {

	private static final long serialVersionUID = 113527588225022202L;

	public JsonToColumns() {
		this(new Params());
	}

	public JsonToColumns(Params params) {
		super(FormatType.JSON, FormatType.COLUMNS, params);
	}
}

