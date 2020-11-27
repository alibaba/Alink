package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.classification.NaiveBayesModelMapper;
import com.alibaba.alink.params.classification.NaiveBayesPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Naive bayes pipeline model.
 */
public class NaiveBayesModel extends MapModel <NaiveBayesModel>
	implements NaiveBayesPredictParams <NaiveBayesModel> {

	private static final long serialVersionUID = 7028563877378956650L;

	public NaiveBayesModel() {
		this(null);
	}

	public NaiveBayesModel(Params params) {
		super(NaiveBayesModelMapper::new, params);
	}
}
