package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTrainBatchOp;
import com.alibaba.alink.params.classification.NaiveBayesPredictParams;
import com.alibaba.alink.params.classification.NaiveBayesTrainParams;
import com.alibaba.alink.pipeline.Trainer;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Naive Bayes classifier is a simple probability classification algorithm using
 * Bayes theorem based on independent assumption. It is an independent feature model.
 * The input feature can be continual or categorical.
 */
public class NaiveBayes extends Trainer<NaiveBayes, NaiveBayesModel>
    implements NaiveBayesTrainParams<NaiveBayes>,
    NaiveBayesPredictParams<NaiveBayes>, HasLazyPrintModelInfo<NaiveBayes> {

    public NaiveBayes() {
        super();
    }

    public NaiveBayes(Params params) {
        super(params);
    }

    @Override
    public BatchOperator train(BatchOperator in) {
        return new NaiveBayesTrainBatchOp(this.params).linkFrom(in);
    }
}
