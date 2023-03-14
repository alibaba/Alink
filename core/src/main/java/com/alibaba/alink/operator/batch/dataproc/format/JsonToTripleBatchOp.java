package com.alibaba.alink.operator.batch.dataproc.format;

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
@NameEn("JSON To Triple")
public class JsonToTripleBatchOp extends AnyToTripleBatchOp <JsonToTripleBatchOp>
	implements JsonToTripleParams <JsonToTripleBatchOp> {

	private static final long serialVersionUID = 602168951085593347L;

	public JsonToTripleBatchOp() {
		this(new Params());
	}

	public JsonToTripleBatchOp(Params params) {
		super(FormatType.JSON, params);
	}
}
