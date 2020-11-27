package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToKvParams;

/**
 * Transform data type from Csv to Kv.
 */
public class CsvToKvBatchOp extends BaseFormatTransBatchOp <CsvToKvBatchOp>
	implements CsvToKvParams <CsvToKvBatchOp> {

	private static final long serialVersionUID = 1219573486418599782L;

	public CsvToKvBatchOp() {
		this(new Params());
	}

	public CsvToKvBatchOp(Params params) {
		super(FormatType.CSV, FormatType.KV, params);
	}
}
