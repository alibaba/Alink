package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToColumnsParams;

/**
 * Transform data type from Vector to Columns.
 */
@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量转列数据")
public class VectorToColumnsStreamOp extends BaseFormatTransStreamOp <VectorToColumnsStreamOp>
	implements VectorToColumnsParams <VectorToColumnsStreamOp> {

	private static final long serialVersionUID = -8146831372562929851L;

	public VectorToColumnsStreamOp() {
		this(new Params());
	}

	public VectorToColumnsStreamOp(Params params) {
		super(FormatType.VECTOR, FormatType.COLUMNS, params);
	}
}
