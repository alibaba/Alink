package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToVectorParams;

/**
 * Transform data type from Json to Vector.
 */
@NameCn("JSON转向量")
public class JsonToVector extends BaseFormatTrans <JsonToVector> implements JsonToVectorParams <JsonToVector> {

	private static final long serialVersionUID = -2384972282659979712L;

	public JsonToVector() {
		this(new Params());
	}

	public JsonToVector(Params params) {
		super(FormatType.JSON, FormatType.VECTOR, params);
	}
}

