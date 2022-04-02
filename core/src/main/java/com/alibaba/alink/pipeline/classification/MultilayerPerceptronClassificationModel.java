package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.classification.ann.MlpcModelMapper;
import com.alibaba.alink.params.classification.MultilayerPerceptronPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Model fitted by MultilayerPerceptronClassifier.
 */
@NameCn("多层感知机模型")
public class MultilayerPerceptronClassificationModel extends MapModel <MultilayerPerceptronClassificationModel>
	implements MultilayerPerceptronPredictParams <MultilayerPerceptronClassificationModel> {

	private static final long serialVersionUID = -537528907364301587L;

	public MultilayerPerceptronClassificationModel() {this(null);}

	public MultilayerPerceptronClassificationModel(Params params) {
		super(MlpcModelMapper::new, params);
	}
}
