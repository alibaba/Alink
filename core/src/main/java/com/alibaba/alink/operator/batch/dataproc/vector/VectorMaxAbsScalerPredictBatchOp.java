package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMaxAbsScalerModelMapper;
import com.alibaba.alink.params.dataproc.vector.VectorMaxAbsScalerPredictParams;

/**
 * MaxAbsScaler transforms a dataSet of rows,rescaling each feature to range
 * [-1, 1] by dividing through the maximum absolute value in each feature.
 * MaxAbsPredict will scale the dataSet with model which trained from MaxAbsTrain.
 */
@InputPorts(values = {@PortSpec(value = PortType.MODEL, suggestions = VectorMaxAbsScalerTrainBatchOp.class), @PortSpec(PortType.DATA)})
@NameCn("向量绝对值最大化预测")
public final class VectorMaxAbsScalerPredictBatchOp extends ModelMapBatchOp <VectorMaxAbsScalerPredictBatchOp>
	implements VectorMaxAbsScalerPredictParams <VectorMaxAbsScalerPredictBatchOp> {

	private static final long serialVersionUID = -3197823937364521012L;

	public VectorMaxAbsScalerPredictBatchOp() {
		this(new Params());
	}

	public VectorMaxAbsScalerPredictBatchOp(Params params) {
		super(VectorMaxAbsScalerModelMapper::new, params);
	}

}
