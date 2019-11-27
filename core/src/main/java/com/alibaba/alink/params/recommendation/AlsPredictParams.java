package com.alibaba.alink.params.recommendation;

import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.recommendation.HasItemCol;
import com.alibaba.alink.params.shared.recommendation.HasUserCol;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface AlsPredictParams<T> extends
    HasUserCol<T>,
    HasItemCol<T>,
    HasPredictionCol<T> {

//    ParamInfo<String> OPERATION = ParamInfoFactory
//        .createParamInfo("operation", String.class)
//        .setDescription("Prediction task, one of \"recommendForUsers\" or \"rating\"")
//        .setHasDefaultValue("rating")
//        .build();
//
//    default String getOperation() {
//        return get(OPERATION);
//    }
//
//    default T setOperation(String value) {
//        return set(OPERATION, value);
//    }

//    ParamInfo<Integer> TOP_K = ParamInfoFactory
//        .createParamInfo("topK", Integer.class)
//        .setDescription("Number of items recommended")
//        .setHasDefaultValue(100)
//        .build();
//
//    default Integer getTopK() {
//        return get(TOP_K);
//    }
//
//    default T setTopK(Integer value) {
//        return set(TOP_K, value);
//    }

}
