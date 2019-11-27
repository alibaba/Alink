package com.alibaba.alink.params.statistics;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.operator.batch.feature.QuantileDiscretizerTrainBatchOp;

/**
 * round mode.
 */
public interface HasRoundMode<T> extends WithParams<T> {
    ParamInfo<String> ROUND_MODE = ParamInfoFactory.createParamInfo("roundMode", String.class)
        .setDescription("when q is the group size, k is the k-th group, total is the sample size, " +
            "then the index of k-th q-quantile is (1.0 / q) * (total - 1) * k. " +
            "if convert index from double to long, it use round mode." +
            "<ul>" +
            "<li>round: [index]</li>" +
            "<li>ceil: ⌈index⌉</li>" +
            "<li>floor: ⌊index⌋</li>" +
            "</ul>" +
            "<p>")
        .setHasDefaultValue(QuantileDiscretizerTrainBatchOp.RoundModeEnum.ROUND.toString())
        .build();

    default String getRoundMode() {
        return get(ROUND_MODE);
    }

    default T setRoundMode(String value) {
        return set(ROUND_MODE, value);
    }
}
