package com.alibaba.alink.operator.local.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithSecondInputSpec;
import com.alibaba.alink.operator.common.dataproc.vector.VectorStandardScalerModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.dataproc.vector.VectorStandardPredictParams;

/**
 * StandardScaler transforms a dataSet, normalizing each feature to have unit standard deviation and/or zero mean.
 */
@InputPorts(values = {@PortSpec(value = PortType.MODEL, suggestions = {VectorStandardScalerTrainLocalOp.class}), @PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ReservedColsWithSecondInputSpec
@NameCn("向量标准化预测")
public final class VectorStandardScalerPredictLocalOp extends ModelMapLocalOp <VectorStandardScalerPredictLocalOp>
	implements VectorStandardPredictParams <VectorStandardScalerPredictLocalOp> {

	public VectorStandardScalerPredictLocalOp() {
		this(new Params());
	}

	public VectorStandardScalerPredictLocalOp(Params params) {
		super(VectorStandardScalerModelMapper::new, params);
	}

}
