package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToTripleParams;

/**
 * Transform data type from Vector to Triple.
 */
@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量转三元组")
@NameEn("Vector to triple")
public class VectorToTripleStreamOp extends AnyToTripleStreamOp <VectorToTripleStreamOp>
	implements VectorToTripleParams <VectorToTripleStreamOp> {

	private static final long serialVersionUID = -6373174913353961089L;

	public VectorToTripleStreamOp() {
		this(new Params());
	}

	public VectorToTripleStreamOp(Params params) {
		super(FormatType.VECTOR, params);
	}
}
