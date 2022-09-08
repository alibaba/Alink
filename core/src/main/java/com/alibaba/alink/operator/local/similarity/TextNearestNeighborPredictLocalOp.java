package com.alibaba.alink.operator.local.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.similarity.TextNearestNeighborTrainBatchOp;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;

/**
 * Find the nearest neighbor of query texts.
 */
@InputPorts(values = {@PortSpec(value = PortType.MODEL, suggestions = TextNearestNeighborTrainBatchOp.class),
	@PortSpec(PortType.DATA)})
@NameCn("文本最近邻预测")
public class TextNearestNeighborPredictLocalOp extends ModelMapLocalOp <TextNearestNeighborPredictLocalOp>
	implements NearestNeighborPredictParams <TextNearestNeighborPredictLocalOp> {

	public TextNearestNeighborPredictLocalOp() {
		this(new Params());
	}

	public TextNearestNeighborPredictLocalOp(Params params) {
		super(NearestNeighborsMapper::new, params);
	}
}
