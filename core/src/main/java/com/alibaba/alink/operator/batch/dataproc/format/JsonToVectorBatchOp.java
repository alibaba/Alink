package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToVectorParams;

/**
 * Transform data type from Json to Vector.
 */
@ParamSelectColumnSpec(name = "jsonCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("JSON转向量")
@NameEn("JSON To Vector")
public class JsonToVectorBatchOp extends BaseFormatTransBatchOp <JsonToVectorBatchOp>
	implements JsonToVectorParams <JsonToVectorBatchOp> {

	private static final long serialVersionUID = 2822599844401661584L;

	public JsonToVectorBatchOp() {
		this(new Params());
	}

	public JsonToVectorBatchOp(Params params) {
		super(FormatType.JSON, FormatType.VECTOR, params);
	}
}
