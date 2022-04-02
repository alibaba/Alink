package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToCsvParams;

/**
 * Transform data type from Vector to Csv.
 */
@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量转CSV")
public class VectorToCsvStreamOp extends BaseFormatTransStreamOp <VectorToCsvStreamOp>
	implements VectorToCsvParams <VectorToCsvStreamOp> {

	private static final long serialVersionUID = 5373033053946898305L;

	public VectorToCsvStreamOp() {
		this(new Params());
	}

	public VectorToCsvStreamOp(Params params) {
		super(FormatType.VECTOR, FormatType.CSV, params);
	}
}
