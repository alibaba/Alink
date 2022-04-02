package com.alibaba.alink.operator.stream.pytorch;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.pytorch.TorchModelPredictMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dl.TorchModelPredictParams;

/**
 * This operator loads TorchScript model and do predictions.
 */
@NameCn("PyTorch模型预测")
public class TorchModelPredictStreamOp extends MapStreamOp <TorchModelPredictStreamOp>
	implements TorchModelPredictParams <TorchModelPredictStreamOp> {

	public TorchModelPredictStreamOp() {
		this(null);
	}

	public TorchModelPredictStreamOp(Params params) {
		super(TorchModelPredictMapper::new, params);
	}
}
