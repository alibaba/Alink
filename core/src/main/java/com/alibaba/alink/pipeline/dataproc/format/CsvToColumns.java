package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToColumnsParams;

/**
 * Transform data type from Csv to Columns.
 */
@NameCn("CSV转列数据")
public class CsvToColumns extends BaseFormatTrans <CsvToColumns> implements CsvToColumnsParams <CsvToColumns> {

	private static final long serialVersionUID = 8763441355972991932L;

	public CsvToColumns() {
		this(new Params());
	}

	public CsvToColumns(Params params) {
		super(FormatType.CSV, FormatType.COLUMNS, params);
	}
}

