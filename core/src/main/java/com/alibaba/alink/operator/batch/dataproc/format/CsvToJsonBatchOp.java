package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToJsonParams;

/**
 * Transform data type from Csv to Json.
 */
@ParamSelectColumnSpec(name = "csvCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("CSV转JSON")
@NameEn("Csv To JSON")
public class CsvToJsonBatchOp extends BaseFormatTransBatchOp <CsvToJsonBatchOp>
	implements CsvToJsonParams <CsvToJsonBatchOp> {

	private static final long serialVersionUID = 2213921418015136997L;

	public CsvToJsonBatchOp() {
		this(new Params());
	}

	public CsvToJsonBatchOp(Params params) {
		super(FormatType.CSV, FormatType.JSON, params);
	}
}
