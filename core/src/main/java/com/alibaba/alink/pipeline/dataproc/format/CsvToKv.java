package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToKvParams;

/**
 * Transform data type from Csv to Kv.
 */
public class CsvToKv extends BaseFormatTrans <CsvToKv> implements CsvToKvParams <CsvToKv> {

	private static final long serialVersionUID = 6224311930122723889L;

	public CsvToKv() {
		this(new Params());
	}

	public CsvToKv(Params params) {
		super(FormatType.CSV, FormatType.KV, params);
	}
}

