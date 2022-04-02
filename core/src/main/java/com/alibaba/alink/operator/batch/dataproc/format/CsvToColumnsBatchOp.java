package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToColumnsParams;

/**
 * Transform data type from Csv to Columns.
 */
@ParamSelectColumnSpec(name = "csvCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("CSV转列数据")
public class CsvToColumnsBatchOp extends BaseFormatTransBatchOp <CsvToColumnsBatchOp>
	implements CsvToColumnsParams <CsvToColumnsBatchOp> {

	private static final long serialVersionUID = 8464866781868383697L;

	public CsvToColumnsBatchOp() {
		this(new Params());
	}

	public CsvToColumnsBatchOp(Params params) {
		super(FormatType.CSV, FormatType.COLUMNS, params);
	}
}
