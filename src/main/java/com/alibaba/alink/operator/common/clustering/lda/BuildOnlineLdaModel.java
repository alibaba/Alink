package com.alibaba.alink.operator.common.clustering.lda;

import com.alibaba.alink.operator.common.clustering.LdaModelData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Build online lda model.
 */
public class BuildOnlineLdaModel extends CompleteResultFunction {
    private int topicNum;
    private double beta;

    /**
     * Constructor.
     * @param topicNum the number of topics.
     * @param beta the beta param.
     */
    public BuildOnlineLdaModel(int topicNum, double beta) {
        this.topicNum = topicNum;
        this.beta = beta;
    }

    @Override
    public List<Row> calc(ComContext context) {
        if (context.getTaskId() != 0) {
            return null;
        }

        Tuple2<Long, Integer> tuple2 = ((List<Tuple2<Long, Integer>>) context.getObj(LdaVariable.shape)).get(0);

        int vocabularySize = tuple2.f1;

        //model
        LdaModelData modelData = new LdaModelData();

        DenseMatrix alpha = context.getObj(LdaVariable.alpha);

        Double[] alphaVec = new Double[alpha.numRows()];
        for (int i = 0; i < alphaVec.length; i++) {
            alphaVec[i] = alpha.get(i, 0);
        }
        modelData.alpha = alphaVec;

        Double[] betaVec = new Double[topicNum];
        Arrays.fill(betaVec, beta);
        modelData.beta = betaVec;

        modelData.topicNum = topicNum;
        modelData.vocabularySize = vocabularySize;
        modelData.wordTopicCounts = context.getObj(LdaVariable.lambda);

        modelData.optimizer = "online";

        long nonEmptyWordCount = Math.round(((double[]) context.getObj(LdaVariable.nonEmptyWordCount))[0]);
        double[] logLikelihoods = context.getObj(LdaVariable.logLikelihood);
        double logPerplexity = -logLikelihoods[0] / nonEmptyWordCount;
        modelData.logLikelihood = logLikelihoods[0];
        modelData.logPerplexity = logPerplexity;
        List<Row> res = new ArrayList<>();
        res.add(Row.of(modelData));
        return res;
    }
}
