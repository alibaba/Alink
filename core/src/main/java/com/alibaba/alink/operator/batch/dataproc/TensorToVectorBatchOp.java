package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.TensorToVectorMapper;
import com.alibaba.alink.params.dataproc.TensorToVectorParams;

/**
 * batch op for tensor to vector.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.NUMERIC_TENSOR_TYPES)
@NameCn("张量转向量")
@NameEn("Tensor To Vector")
public class TensorToVectorBatchOp extends MapBatchOp <TensorToVectorBatchOp>
	implements TensorToVectorParams <TensorToVectorBatchOp> {

	public TensorToVectorBatchOp() {
		this(new Params());
	}

	public TensorToVectorBatchOp(Params params) {
		super(TensorToVectorMapper::new, params);
	}

}
