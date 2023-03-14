package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToVectorParams;

/**
 * Transform data type from Csv to Vector.
 */
@ParamSelectColumnSpec(name = "csvCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("CSV转向量")
@NameEn("Csv To Vector")
public class CsvToVectorBatchOp extends BaseFormatTransBatchOp <CsvToVectorBatchOp>
	implements CsvToVectorParams <CsvToVectorBatchOp> {

	private static final long serialVersionUID = 8595417565169304131L;

	public CsvToVectorBatchOp() {
		this(new Params());
	}

	public CsvToVectorBatchOp(Params params) {
		super(FormatType.CSV, FormatType.VECTOR, params);
	}
}
