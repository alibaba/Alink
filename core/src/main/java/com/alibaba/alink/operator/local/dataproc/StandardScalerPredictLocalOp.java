package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.common.dataproc.StandardScalerModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.dataproc.StandardPredictParams;

/**
 * StandardScaler transforms a dataset, normalizing each feature to have unit standard deviation and/or zero mean.
 */
@InputPorts(values = {
	@PortSpec(value = PortType.MODEL, desc = PortDesc.PREDICT_INPUT_MODEL,
		suggestions = StandardScalerTrainLocalOp.class),
	@PortSpec(value = PortType.DATA, desc = PortDesc.PREDICT_INPUT_DATA)
})
@NameCn("标准化批预测")
public final class StandardScalerPredictLocalOp extends ModelMapLocalOp <StandardScalerPredictLocalOp>
	implements StandardPredictParams <StandardScalerPredictLocalOp> {

	public StandardScalerPredictLocalOp() {
		this(new Params());
	}

	public StandardScalerPredictLocalOp(Params params) {
		super(StandardScalerModelMapper::new, params);
	}

}
