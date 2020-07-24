package com.alibaba.alink.operator.stream.classification;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.classification.NaiveBayesModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.NaiveBayesPredictParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Naive Bayes Predictor.
 */
public class NaiveBayesPredictStreamOp extends ModelMapStreamOp<NaiveBayesPredictStreamOp>
    implements NaiveBayesPredictParams<NaiveBayesPredictStreamOp> {

    public NaiveBayesPredictStreamOp(BatchOperator model) {
        this(model, new Params());
    }

    public NaiveBayesPredictStreamOp(BatchOperator model, Params params) {
        super(model, NaiveBayesModelMapper::new, params);
    }
}
