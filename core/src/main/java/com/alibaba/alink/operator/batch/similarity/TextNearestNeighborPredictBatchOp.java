package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;

/**
 * Find the nearest neighbor of query texts.
 */
@InputPorts(values = {@PortSpec(value = PortType.MODEL, suggestions = TextNearestNeighborTrainBatchOp.class), @PortSpec(PortType.DATA)})
@NameCn("文本最近邻预测")
@NameEn("Text Nearest Neighbor Prediction")
public class TextNearestNeighborPredictBatchOp extends ModelMapBatchOp <TextNearestNeighborPredictBatchOp>
	implements NearestNeighborPredictParams <TextNearestNeighborPredictBatchOp> {

	private static final long serialVersionUID = -464374820231352727L;

	public TextNearestNeighborPredictBatchOp() {
		this(new Params());
	}

	public TextNearestNeighborPredictBatchOp(Params params) {
		super(NearestNeighborsMapper::new, params);
	}
}
