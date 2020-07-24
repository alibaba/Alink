package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.common.classification.NaiveBayesModelMapper;
import com.alibaba.alink.params.classification.NaiveBayesPredictParams;
import com.alibaba.alink.pipeline.MapModel;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Naive bayes pipeline model.
 */
public class NaiveBayesModel extends MapModel<NaiveBayesModel>
    implements NaiveBayesPredictParams<NaiveBayesModel> {

    public NaiveBayesModel() {
        this(null);
    }

    public NaiveBayesModel(Params params) {
        super(NaiveBayesModelMapper::new, params);
    }
}
