package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToCsvParams;

/**
 * Transform data type from Json to Csv.
 */
public class JsonToCsv extends BaseFormatTrans <JsonToCsv> implements JsonToCsvParams <JsonToCsv> {

	private static final long serialVersionUID = -6745422357930920407L;

	public JsonToCsv() {
		this(new Params());
	}

	public JsonToCsv(Params params) {
		super(FormatType.JSON, FormatType.CSV, params);
	}
}

