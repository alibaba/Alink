package com.alibaba.alink.operator.batch.classification;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.classification.NaiveBayesModelMapper;
import com.alibaba.alink.params.classification.NaiveBayesPredictParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Naive Bayes Predictor.
 */
public class NaiveBayesPredictBatchOp extends ModelMapBatchOp<NaiveBayesPredictBatchOp>
    implements NaiveBayesPredictParams<NaiveBayesPredictBatchOp> {

    public NaiveBayesPredictBatchOp() {
        this(null);
    }

    public NaiveBayesPredictBatchOp(Params params) {
        super(NaiveBayesModelMapper::new, params);
    }
}
