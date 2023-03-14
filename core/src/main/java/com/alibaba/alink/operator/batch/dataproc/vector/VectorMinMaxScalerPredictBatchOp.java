package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMinMaxScalerModelMapper;
import com.alibaba.alink.params.dataproc.vector.VectorMinMaxScalerPredictParams;

/**
 * MinMaxScaler transforms a dataSet of rows, rescaling each feature
 * to a specific range [min, max). (often [0, 1]).
 * MinMaxScalerPredict will scale the dataSet with model which trained from MaxAbsTrain.
 */
@InputPorts(values = {@PortSpec(value = PortType.MODEL, suggestions = VectorMinMaxScalerTrainBatchOp.class), @PortSpec(PortType.DATA)})
@NameCn("向量归一化预测")
@NameEn("Vector MinAbs Scaler Prediction")
public final class VectorMinMaxScalerPredictBatchOp extends ModelMapBatchOp <VectorMinMaxScalerPredictBatchOp>
	implements VectorMinMaxScalerPredictParams <VectorMinMaxScalerPredictBatchOp> {

	private static final long serialVersionUID = -2138304770669452493L;

	public VectorMinMaxScalerPredictBatchOp() {
		this(new Params());
	}

	public VectorMinMaxScalerPredictBatchOp(Params params) {
		super(VectorMinMaxScalerModelMapper::new, params);
	}

}
