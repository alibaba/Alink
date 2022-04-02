package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToJsonParams;

/**
 * Transform data type from Csv to Json.
 */
@NameCn("CSV转JSON")
public class CsvToJson extends BaseFormatTrans <CsvToJson> implements CsvToJsonParams <CsvToJson> {

	private static final long serialVersionUID = -2643594277411636335L;

	public CsvToJson() {
		this(new Params());
	}

	public CsvToJson(Params params) {
		super(FormatType.CSV, FormatType.JSON, params);
	}
}

