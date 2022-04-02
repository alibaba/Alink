package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.VectorToTensorMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.VectorToTensorParams;

/**
 * stream op for tensor to vector.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量转张量")
public class VectorToTensorStreamOp extends MapStreamOp <VectorToTensorStreamOp>
	implements VectorToTensorParams <VectorToTensorStreamOp> {

	public VectorToTensorStreamOp() {
		this(new Params());
	}

	public VectorToTensorStreamOp(Params params) {
		super(VectorToTensorMapper::new, params);
	}

}
