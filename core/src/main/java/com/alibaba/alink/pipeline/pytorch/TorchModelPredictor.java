package com.alibaba.alink.pipeline.pytorch;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.pytorch.TorchModelPredictMapper;
import com.alibaba.alink.params.dl.TorchModelPredictParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * This operator loads a TorchScript model, and performs prediction with PyTorch's Java SDK.
 */
@NameCn("TorchScript 模型预测")
public class TorchModelPredictor extends MapTransformer <TorchModelPredictor>
	implements TorchModelPredictParams <TorchModelPredictor> {

	public TorchModelPredictor() {this(null);}

	public TorchModelPredictor(Params params) {
		super(TorchModelPredictMapper::new, params);
	}
}
