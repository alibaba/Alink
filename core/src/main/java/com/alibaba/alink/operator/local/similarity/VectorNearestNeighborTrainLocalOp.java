package com.alibaba.alink.operator.local.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.similarity.BaseNearestNeighborTrainBatchOp;
import com.alibaba.alink.operator.common.similarity.TrainType;
import com.alibaba.alink.params.similarity.VectorNearestNeighborTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Find the nearest neighbor of query vectors.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量最近邻训练")
@NameEn("Vector Nearest Neighbor Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.similarity.VectorNearestNeighbor")
public class VectorNearestNeighborTrainLocalOp
	extends BaseNearestNeighborTrainLocalOp<VectorNearestNeighborTrainLocalOp>
	implements VectorNearestNeighborTrainParams<VectorNearestNeighborTrainLocalOp> {

	public VectorNearestNeighborTrainLocalOp() {
		this(new Params());
	}

	public VectorNearestNeighborTrainLocalOp(Params params) {
		super(params.set(BaseNearestNeighborTrainBatchOp.TRAIN_TYPE, TrainType.VECTOR));
	}
}
