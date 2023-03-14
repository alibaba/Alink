package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToJsonParams;

/**
 * Transform data type from Vector to Json.
 */
@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量转JSON")
@NameEn("Vector to Json")
public class VectorToJsonBatchOp extends BaseFormatTransBatchOp <VectorToJsonBatchOp>
	implements VectorToJsonParams <VectorToJsonBatchOp> {

	private static final long serialVersionUID = 6232989400260500967L;

	public VectorToJsonBatchOp() {
		this(new Params());
	}

	public VectorToJsonBatchOp(Params params) {
		super(FormatType.VECTOR, FormatType.JSON, params);
	}
}
