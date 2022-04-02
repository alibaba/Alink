package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.TensorToVectorMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.TensorToVectorParams;

/**
 * stream op for tensor to vector.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.NUMERIC_TENSOR_TYPES)
@NameCn("张量转向量")
public class TensorToVectorStreamOp extends MapStreamOp <TensorToVectorStreamOp>
	implements TensorToVectorParams <TensorToVectorStreamOp> {

	public TensorToVectorStreamOp() {
		this(new Params());
	}

	public TensorToVectorStreamOp(Params params) {
		super(TensorToVectorMapper::new, params);
	}

}
