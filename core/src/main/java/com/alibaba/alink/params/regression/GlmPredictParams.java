package com.alibaba.alink.params.regression;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.shared.colname.HasPredictionCol;

/**
 * Parameter of glm predict.
 */
public interface GlmPredictParams<T> extends
    HasPredictionCol<T> {

    ParamInfo<String> LINK_PRED_RESULT_COL = ParamInfoFactory
        .createParamInfo("linkPredResultCol", String.class)
        .setDescription("link predict col name of output")
        .setAlias(new String[]{"linkPredResultColName"})
        .setHasDefaultValue(null)
        .build();

    default String getLinkPredResultCol() {
        return get(LINK_PRED_RESULT_COL);
    }

    default T setLinkPredResultCol(String value) {
        return set(LINK_PRED_RESULT_COL, value);
    }

}
