package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToTripleParams;

/**
 * Transform data type from Json to Triple.
 */
@ParamSelectColumnSpec(name = "jsonCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("JSON转三元组")
@NameEn("Json to triple")
public class JsonToTripleStreamOp extends AnyToTripleStreamOp <JsonToTripleStreamOp>
	implements JsonToTripleParams <JsonToTripleStreamOp> {

	private static final long serialVersionUID = 8653473208180902492L;

	public JsonToTripleStreamOp() {
		this(new Params());
	}

	public JsonToTripleStreamOp(Params params) {
		super(FormatType.JSON, params);
	}
}
