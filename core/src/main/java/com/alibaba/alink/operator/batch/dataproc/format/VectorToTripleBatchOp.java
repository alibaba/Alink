package com.alibaba.alink.operator.batch.dataproc.format;

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
@NameEn("Vector to Triple")
public class VectorToTripleBatchOp extends AnyToTripleBatchOp <VectorToTripleBatchOp>
	implements VectorToTripleParams <VectorToTripleBatchOp> {

	private static final long serialVersionUID = 2762730453738335970L;

	public VectorToTripleBatchOp() {
		this(new Params());
	}

	public VectorToTripleBatchOp(Params params) {
		super(FormatType.VECTOR, params);
	}

}
