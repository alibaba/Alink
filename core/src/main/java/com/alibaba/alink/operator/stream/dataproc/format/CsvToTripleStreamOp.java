package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToTripleParams;

/**
 * Transform data type from Csv to Triple.
 */
@ParamSelectColumnSpec(name = "csvCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("CSV转三元组")
public class CsvToTripleStreamOp extends AnyToTripleStreamOp <CsvToTripleStreamOp>
	implements CsvToTripleParams <CsvToTripleStreamOp> {

	private static final long serialVersionUID = 4904881842860650157L;

	public CsvToTripleStreamOp() {
		this(new Params());
	}

	public CsvToTripleStreamOp(Params params) {
		super(FormatType.CSV, params);
	}
}
