package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.common.classification.NaiveBayesTextModelMapper;
import com.alibaba.alink.params.classification.NaiveBayesTextPredictParams;
import com.alibaba.alink.pipeline.MapModel;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Text Naive bayes pipeline model.
 */
public class NaiveBayesTextModel extends MapModel<NaiveBayesTextModel>
	implements NaiveBayesTextPredictParams<NaiveBayesTextModel> {

	public NaiveBayesTextModel() {this(null);}

	public NaiveBayesTextModel(Params params) {
		super(NaiveBayesTextModelMapper::new, params);
	}

}
