package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToCsvParams;

/**
 * Transform data type from Json to Csv.
 */
@ParamSelectColumnSpec(name = "jsonCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("JSON转CSV")
@NameEn("Json to csv")
public class JsonToCsvStreamOp extends BaseFormatTransStreamOp <JsonToCsvStreamOp>
	implements JsonToCsvParams <JsonToCsvStreamOp> {

	private static final long serialVersionUID = -238287281566741104L;

	public JsonToCsvStreamOp() {
		this(new Params());
	}

	public JsonToCsvStreamOp(Params params) {
		super(FormatType.JSON, FormatType.CSV, params);
	}
}
