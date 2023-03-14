package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;

/**
 * Find the nearest neighbor of query vectors.
 */
@InputPorts(values = {@PortSpec(value = PortType.MODEL, suggestions = VectorNearestNeighborTrainBatchOp.class), @PortSpec(PortType.DATA)})
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量最近邻预测")
@NameEn("Vector Nearest Neighbor Prediction")
public class VectorNearestNeighborPredictBatchOp extends ModelMapBatchOp <VectorNearestNeighborPredictBatchOp>
	implements NearestNeighborPredictParams <VectorNearestNeighborPredictBatchOp> {

	private static final long serialVersionUID = 6555529390162333788L;

	public VectorNearestNeighborPredictBatchOp() {
		this(new Params());
	}

	public VectorNearestNeighborPredictBatchOp(Params params) {
		super(NearestNeighborsMapper::new, params);
	}
}
