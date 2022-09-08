package com.alibaba.alink.operator.local.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.timeseries.DeepARModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.timeseries.DeepARPredictParams;

@NameCn("DeepAR预测")
public class DeepARPredictLocalOp extends ModelMapLocalOp <DeepARPredictLocalOp>
	implements DeepARPredictParams <DeepARPredictLocalOp> {

	public DeepARPredictLocalOp() {
		this(new Params());
	}

	public DeepARPredictLocalOp(Params params) {
		super(DeepARModelMapper::new, params);
	}
}
