package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToCsvParams;

/**
 * Transform data type from Json to Csv.
 */
@ParamSelectColumnSpec(name = "jsonCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("JSON转CSV")
public class JsonToCsvBatchOp extends BaseFormatTransBatchOp <JsonToCsvBatchOp>
	implements JsonToCsvParams <JsonToCsvBatchOp> {

	private static final long serialVersionUID = 2351101769821357504L;

	public JsonToCsvBatchOp() {
		this(new Params());
	}

	public JsonToCsvBatchOp(Params params) {
		super(FormatType.JSON, FormatType.CSV, params);
	}
}
