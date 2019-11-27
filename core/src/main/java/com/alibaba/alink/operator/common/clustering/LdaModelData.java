package com.alibaba.alink.operator.common.clustering;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.common.nlp.DocCountVectorizerModelData;

import java.io.Serializable;
import java.util.List;

/**
 * The model data of lda algorithm.
 */
public class LdaModelData implements Serializable  {
    public LdaModelData() {}

    public LdaModelData(int topicNum, int vocabularySize, DenseMatrix gamma, Double[] alpha, Double[] beta) {
        this.topicNum = topicNum;
        this.vocabularySize = vocabularySize;
        this.gamma = gamma;
        this.alpha = alpha;
        this.beta = beta;
    }
    //wordTopicCounts and topicCounts
    public DenseMatrix gamma;
    public DenseMatrix wordTopicCounts;
    public Double[] alpha;
    public Double[] beta;
    public int topicNum;
    public int vocabularySize;
    //the optimizer strategy.
    public String optimizer = "em";
    //for calculate perplexity.
    public double logPerplexity;
    public double logLikelihood;

    //doc count data.
    public List<String> list;
}
