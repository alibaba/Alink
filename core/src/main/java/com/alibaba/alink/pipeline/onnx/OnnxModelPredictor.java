package com.alibaba.alink.pipeline.onnx;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.onnx.OnnxModelPredictMapper;
import com.alibaba.alink.params.onnx.OnnxModelPredictParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * This operator loads an ONNX model, and performs prediction with ONNX's Java SDK.
 */
@NameCn("ONNX 模型预测")
@NameEn("ONNX Model Predictor")
public class OnnxModelPredictor extends MapTransformer <OnnxModelPredictor>
	implements OnnxModelPredictParams <OnnxModelPredictor> {

	public OnnxModelPredictor() {this(null);}

	public OnnxModelPredictor(Params params) {
		super(OnnxModelPredictMapper::new, params);
	}
}
