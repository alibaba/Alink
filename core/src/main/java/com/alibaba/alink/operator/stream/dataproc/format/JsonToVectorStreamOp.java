package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToVectorParams;

/**
 * Transform data type from Json to Vector.
 */
@ParamSelectColumnSpec(name = "jsonCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("JSON转向量")
public class JsonToVectorStreamOp extends BaseFormatTransStreamOp <JsonToVectorStreamOp>
	implements JsonToVectorParams <JsonToVectorStreamOp> {

	private static final long serialVersionUID = 245366859720055901L;

	public JsonToVectorStreamOp() {
		this(new Params());
	}

	public JsonToVectorStreamOp(Params params) {
		super(FormatType.JSON, FormatType.VECTOR, params);
	}
}
