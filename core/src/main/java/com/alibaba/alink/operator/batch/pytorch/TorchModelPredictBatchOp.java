package com.alibaba.alink.operator.batch.pytorch;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.pytorch.TorchModelPredictMapper;
import com.alibaba.alink.params.dl.TorchModelPredictParams;

/**
 * This operator loads TorchScript model and do predictions.
 */
@NameCn("PyTorch模型预测")
public class TorchModelPredictBatchOp extends MapBatchOp <TorchModelPredictBatchOp>
	implements TorchModelPredictParams <TorchModelPredictBatchOp> {

	public TorchModelPredictBatchOp() {
		this(null);
	}

	public TorchModelPredictBatchOp(Params params) {
		super(TorchModelPredictMapper::new, params);
	}
}
