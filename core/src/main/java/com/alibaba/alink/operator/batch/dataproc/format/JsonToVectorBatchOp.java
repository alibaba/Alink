package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToVectorParams;

/**
 * Transform data type from Json to Vector.
 */
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
