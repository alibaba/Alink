package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithSecondInputSpec;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorStandardScalerModelMapper;
import com.alibaba.alink.params.dataproc.vector.VectorStandardPredictParams;

/**
 * StandardScaler transforms a dataSet, normalizing each feature to have unit standard deviation and/or zero mean.
 */
@InputPorts(values = {@PortSpec(value = PortType.MODEL, suggestions = {VectorStandardScalerTrainBatchOp.class}), @PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ReservedColsWithSecondInputSpec
@NameCn("向量标准化预测")
public final class VectorStandardScalerPredictBatchOp extends ModelMapBatchOp <VectorStandardScalerPredictBatchOp>
	implements VectorStandardPredictParams <VectorStandardScalerPredictBatchOp> {

	private static final long serialVersionUID = -4397747525977583853L;

	public VectorStandardScalerPredictBatchOp() {
		this(new Params());
	}

	public VectorStandardScalerPredictBatchOp(Params params) {
		super(VectorStandardScalerModelMapper::new, params);
	}

}
