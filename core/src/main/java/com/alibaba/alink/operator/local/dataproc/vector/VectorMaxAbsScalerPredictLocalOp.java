package com.alibaba.alink.operator.local.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMaxAbsScalerModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.dataproc.vector.VectorMaxAbsScalerPredictParams;

/**
 * MaxAbsScaler transforms a dataSet of rows,rescaling each feature to range
 * [-1, 1] by dividing through the maximum absolute value in each feature.
 * MaxAbsPredict will scale the dataSet with model which trained from MaxAbsTrain.
 */
@InputPorts(values = {@PortSpec(value = PortType.MODEL, suggestions = VectorMaxAbsScalerTrainLocalOp.class), @PortSpec(PortType.DATA)})
@NameCn("向量绝对值最大化预测")
public final class VectorMaxAbsScalerPredictLocalOp extends ModelMapLocalOp <VectorMaxAbsScalerPredictLocalOp>
	implements VectorMaxAbsScalerPredictParams <VectorMaxAbsScalerPredictLocalOp> {

	public VectorMaxAbsScalerPredictLocalOp() {
		this(new Params());
	}

	public VectorMaxAbsScalerPredictLocalOp(Params params) {
		super(VectorMaxAbsScalerModelMapper::new, params);
	}

}
