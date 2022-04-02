package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.classification.NaiveBayesTextModelMapper;
import com.alibaba.alink.params.classification.NaiveBayesTextPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Text Naive bayes pipeline model.
 */
@NameCn("朴素贝叶斯文本分类模型")
public class NaiveBayesTextModel extends MapModel <NaiveBayesTextModel>
	implements NaiveBayesTextPredictParams <NaiveBayesTextModel> {

	private static final long serialVersionUID = 548280158926976702L;

	public NaiveBayesTextModel() {this(null);}

	public NaiveBayesTextModel(Params params) {
		super(NaiveBayesTextModelMapper::new, params);
	}

}
