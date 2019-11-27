package com.alibaba.alink.operator.common.clustering.lda;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface LdaEvaluateParams<T> extends WithParams<T> {
    ParamInfo<Double> LOG_PERPLEXITY = ParamInfoFactory
        .createParamInfo("logPerplexity", Double.class)
        .setDescription("logPerplexity")
        .build();

    ParamInfo <Double> LOG_LIKELIHOOD = ParamInfoFactory
        .createParamInfo("logLikelihood", Double.class)
        .setDescription("logLikelihood")
        .build();
}
