package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToColumnsParams;

/**
 * Transform data type from Json to Columns.
 */
public class JsonToColumnsBatchOp extends BaseFormatTransBatchOp <JsonToColumnsBatchOp>
	implements JsonToColumnsParams <JsonToColumnsBatchOp> {

	private static final long serialVersionUID = -6626291824887448264L;

	public JsonToColumnsBatchOp() {
		this(new Params());
	}

	public JsonToColumnsBatchOp(Params params) {
		super(FormatType.JSON, FormatType.COLUMNS, params);
	}
}
