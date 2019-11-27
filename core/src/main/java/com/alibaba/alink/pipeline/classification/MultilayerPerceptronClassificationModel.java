package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.common.classification.ann.MlpcModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.classification.MultilayerPerceptronPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Model fitted by MultilayerPerceptronClassifier.
 */
public class MultilayerPerceptronClassificationModel extends MapModel<MultilayerPerceptronClassificationModel>
	implements MultilayerPerceptronPredictParams <MultilayerPerceptronClassificationModel> {

	public MultilayerPerceptronClassificationModel() {this(null);}

	public MultilayerPerceptronClassificationModel(Params params) {
		super(MlpcModelMapper::new, params);
	}
}
