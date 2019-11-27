package com.alibaba.alink.params.recommendation;

import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.recommendation.HasUserCol;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface AlsTopKPredictParams<T> extends
    HasUserCol<T>,
    HasPredictionCol<T> {

    ParamInfo<Integer> TOP_K = ParamInfoFactory
        .createParamInfo("topK", Integer.class)
        .setDescription("Number of items recommended")
        .setHasDefaultValue(100)
        .build();

    default Integer getTopK() {
        return get(TOP_K);
    }

    default T setTopK(Integer value) {
        return set(TOP_K, value);
    }

}
