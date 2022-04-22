package com.alibaba.alink.operator.stream.onnx;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.onnx.OnnxModelPredictMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.onnx.OnnxModelPredictParams;

/**
 * This operator loads a ONNX model, and performs prediction with ONNX's Java sdk.
 */
@NameCn("ONNX模型预测")
@NameEn("ONNX Model Predictor")
public final class OnnxModelPredictStreamOp extends MapStreamOp <OnnxModelPredictStreamOp>
	implements OnnxModelPredictParams <OnnxModelPredictStreamOp> {

	public OnnxModelPredictStreamOp() {
		this(new Params());
	}

	public OnnxModelPredictStreamOp(Params params) {
		super(OnnxModelPredictMapper::new, params);
	}
}
