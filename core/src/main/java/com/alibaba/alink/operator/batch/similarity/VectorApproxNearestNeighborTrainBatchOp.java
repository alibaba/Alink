package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.similarity.TrainType;
import com.alibaba.alink.params.similarity.VectorApproxNearestNeighborTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Find the approximate nearest neighbor of query vectors.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量近似最近邻训练")
@NameEn("Vector Approx Nearest Neighbor Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.similarity.VectorApproxNearestNeighbor")
public class VectorApproxNearestNeighborTrainBatchOp
	extends BaseNearestNeighborTrainBatchOp <VectorApproxNearestNeighborTrainBatchOp>
	implements VectorApproxNearestNeighborTrainParams<VectorApproxNearestNeighborTrainBatchOp> {

	private static final long serialVersionUID = 1109448101139137342L;

	public VectorApproxNearestNeighborTrainBatchOp() {
		this(new Params());
	}

	public VectorApproxNearestNeighborTrainBatchOp(Params params) {
		super(params.set(BaseNearestNeighborTrainBatchOp.TRAIN_TYPE, TrainType.APPROX_VECTOR));
	}
}
