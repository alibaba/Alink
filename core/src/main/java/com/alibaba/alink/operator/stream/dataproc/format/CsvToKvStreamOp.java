package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToKvParams;

/**
 * Transform data type from Csv to Kv.
 */
@ParamSelectColumnSpec(name = "csvCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("CSV转KV")
@NameEn("Csv to key-value pairs")
public class CsvToKvStreamOp extends BaseFormatTransStreamOp <CsvToKvStreamOp>
	implements CsvToKvParams <CsvToKvStreamOp> {

	private static final long serialVersionUID = -7915067096965151580L;

	public CsvToKvStreamOp() {
		this(new Params());
	}

	public CsvToKvStreamOp(Params params) {
		super(FormatType.CSV, FormatType.KV, params);
	}
}
