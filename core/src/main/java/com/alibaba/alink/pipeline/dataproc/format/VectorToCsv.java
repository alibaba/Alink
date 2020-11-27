package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToCsvParams;

/**
 * Transform data type from Vector to Csv.
 */
public class VectorToCsv extends BaseFormatTrans <VectorToCsv> implements VectorToCsvParams <VectorToCsv> {

	private static final long serialVersionUID = -3896458488194164642L;

	public VectorToCsv() {
		this(new Params());
	}

	public VectorToCsv(Params params) {
		super(FormatType.VECTOR, FormatType.CSV, params);
	}
}

