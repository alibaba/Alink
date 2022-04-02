package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.TripleToVectorParams;

/**
 * Transform data type from Triple to Vector.
 */
@ParamSelectColumnSpec(name = "tripleColumnCol", allowedTypeCollections = TypeCollections.INT_LONG_TYPES)
@ParamSelectColumnSpec(name = "tripleValueCol", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("三元组转向量")
public class TripleToVectorBatchOp extends TripleToAnyBatchOp <TripleToVectorBatchOp>
	implements TripleToVectorParams <TripleToVectorBatchOp> {

	private static final long serialVersionUID = 1983797409096225871L;

	public TripleToVectorBatchOp() {
		this(new Params());
	}

	public TripleToVectorBatchOp(Params params) {
		super(FormatType.VECTOR, params);
	}
}
