package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToJsonParams;

/**
 * Transform data type from Vector to Json.
 */
public class VectorToJson extends BaseFormatTrans <VectorToJson> implements VectorToJsonParams <VectorToJson> {

	private static final long serialVersionUID = 8764333870720402518L;

	public VectorToJson() {
		this(new Params());
	}

	public VectorToJson(Params params) {
		super(FormatType.VECTOR, FormatType.JSON, params);
	}
}

