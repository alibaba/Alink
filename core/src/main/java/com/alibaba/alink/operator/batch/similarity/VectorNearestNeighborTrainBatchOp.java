package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
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
public class VectorNearestNeighborTrainBatchOp
	extends BaseNearestNeighborTrainBatchOp <VectorNearestNeighborTrainBatchOp>
	implements VectorNearestNeighborTrainParams<VectorNearestNeighborTrainBatchOp> {

	private static final long serialVersionUID = 2532932064450519601L;

	public VectorNearestNeighborTrainBatchOp() {
		this(new Params());
	}

	public VectorNearestNeighborTrainBatchOp(Params params) {
		super(params.set(BaseNearestNeighborTrainBatchOp.TRAIN_TYPE, TrainType.VECTOR));
	}
}
