package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.VectorToTensorMapper;
import com.alibaba.alink.params.dataproc.VectorToTensorParams;

/**
 * batch op for tensor to vector.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量转张量")
public class VectorToTensorBatchOp extends MapBatchOp <VectorToTensorBatchOp>
	implements VectorToTensorParams <VectorToTensorBatchOp> {

	public VectorToTensorBatchOp() {
		this(new Params());
	}

	public VectorToTensorBatchOp(Params params) {
		super(VectorToTensorMapper::new, params);
	}

}
