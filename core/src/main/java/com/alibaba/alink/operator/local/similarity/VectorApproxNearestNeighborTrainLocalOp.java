package com.alibaba.alink.operator.local.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.similarity.TrainType;
import com.alibaba.alink.params.similarity.VectorApproxNearestNeighborLocalTrainParams;

/**
 * Find the approximate nearest neighbor of query vectors.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量近似最近邻训练")
public class VectorApproxNearestNeighborTrainLocalOp
	extends BaseNearestNeighborTrainLocalOp <VectorApproxNearestNeighborTrainLocalOp>
	implements VectorApproxNearestNeighborLocalTrainParams <VectorApproxNearestNeighborTrainLocalOp> {

	public VectorApproxNearestNeighborTrainLocalOp() {
		this(new Params());
	}

	public VectorApproxNearestNeighborTrainLocalOp(Params params) {
		super(params.set(BaseNearestNeighborTrainLocalOp.TRAIN_TYPE, TrainType.APPROX_VECTOR));
	}
}
