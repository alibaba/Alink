package com.alibaba.alink.operator.local.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMinMaxScalerModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.dataproc.vector.VectorMinMaxScalerPredictParams;

/**
 * MinMaxScaler transforms a dataSet of rows, rescaling each feature
 * to a specific range [min, max). (often [0, 1]).
 * MinMaxScalerPredict will scale the dataSet with model which trained from MaxAbsTrain.
 */
@InputPorts(values = {@PortSpec(value = PortType.MODEL, suggestions = VectorMinMaxScalerTrainLocalOp.class), @PortSpec(PortType.DATA)})
@NameCn("向量归一化预测")
public final class VectorMinMaxScalerPredictLocalOp extends ModelMapLocalOp <VectorMinMaxScalerPredictLocalOp>
	implements VectorMinMaxScalerPredictParams <VectorMinMaxScalerPredictLocalOp> {

	public VectorMinMaxScalerPredictLocalOp() {
		this(new Params());
	}

	public VectorMinMaxScalerPredictLocalOp(Params params) {
		super(VectorMinMaxScalerModelMapper::new, params);
	}

}
