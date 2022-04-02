package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToVectorParams;

/**
 * Transform data type from Csv to Vector.
 */
@NameCn("CSV转向量")
public class CsvToVector extends BaseFormatTrans <CsvToVector> implements CsvToVectorParams <CsvToVector> {

	private static final long serialVersionUID = -8143610645888840063L;

	public CsvToVector() {
		this(new Params());
	}

	public CsvToVector(Params params) {
		super(FormatType.CSV, FormatType.VECTOR, params);
	}
}

