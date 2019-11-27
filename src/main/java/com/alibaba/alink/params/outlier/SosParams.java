package com.alibaba.alink.params.outlier;

import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface SosParams<T> extends
    HasVectorCol<T>,
    HasPredictionCol<T> {

    ParamInfo<Double> PERPLEXITY = ParamInfoFactory
        .createParamInfo("perplexity", Double.class)
        .setDescription("Perplexity")
        .setHasDefaultValue(4.0)
        .build();

    default Double getPerplexity() {
        return get(PERPLEXITY);
    }

    default T setPerplexity(Double value) {
        return set(PERPLEXITY, value);
    }

}
