package com.alibaba.alink.operator.common.clustering.lda;

import com.alibaba.alink.operator.common.clustering.LdaModelDataConverter;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.Row;

import java.util.List;

public class LdaModelInfo {
    private double logPerplexity;
    private double logLikelihood;
    private int topicNum;
    private int vocabularySize;

    public LdaModelInfo(List<Row> rows) {
        Tuple4<Double, Double, Integer, Integer> perplexity = new LdaModelDataConverter().loadSummary(rows);
        this.logPerplexity = perplexity.f0;
        this.logLikelihood = perplexity.f1;
        this.topicNum = perplexity.f2;
        this.vocabularySize = perplexity.f3;
    }

    public double getLogPerplexity() {
        return logPerplexity;
    }

    public double getLogLikelihood() {
        return logLikelihood;
    }

    public double getTopicNum() {
        return topicNum;
    }

    public double getVocabularySize() {
        return vocabularySize;
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();
        res.append(PrettyDisplayUtils.displayHeadline("LdaModelInfo", '-')+"\n");
        res.append(PrettyDisplayUtils.displayHeadline("logPerplexity information", '=')+"\n");
        res.append("logPerplexity: " + getLogPerplexity() + "\n");
        res.append(PrettyDisplayUtils.displayHeadline("logLikelihood information", '=')+"\n");
        res.append("logLikelihood: " + getLogLikelihood() + '\n');
        res.append(PrettyDisplayUtils.displayHeadline("topic number information", '=')+"\n");
        res.append("topicNum: " + getTopicNum() + '\n');
        res.append(PrettyDisplayUtils.displayHeadline("vocabulary size information", '=')+"\n");
        res.append("vocabularySize: " + getVocabularySize() + '\n');
        return res.toString();
    }
}
