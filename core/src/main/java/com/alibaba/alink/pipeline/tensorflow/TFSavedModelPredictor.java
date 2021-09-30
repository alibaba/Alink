package com.alibaba.alink.pipeline.tensorflow;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tensorflow.TFSavedModelPredictMapper;
import com.alibaba.alink.params.tensorflow.savedmodel.TFSavedModelPredictParams;
import com.alibaba.alink.pipeline.MapTransformer;

public class TFSavedModelPredictor extends MapTransformer <TFSavedModelPredictor>
	implements TFSavedModelPredictParams <TFSavedModelPredictor> {

	public TFSavedModelPredictor() {this(null);}

	public TFSavedModelPredictor(Params params) {
		super(TFSavedModelPredictMapper::new, params);
	}
}
