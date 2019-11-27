package com.alibaba.alink.params.recommendation;

import com.alibaba.alink.params.shared.iter.HasNumIterDefaultAs10;
import com.alibaba.alink.params.shared.recommendation.HasItemCol;
import com.alibaba.alink.params.shared.recommendation.HasRateCol;
import com.alibaba.alink.params.shared.recommendation.HasUserCol;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface AlsTrainParams<T> extends
    HasUserCol<T>,
    HasItemCol<T>,
    HasRateCol<T>,
    HasNumIterDefaultAs10<T> {
    ParamInfo<Integer> RANK = ParamInfoFactory
        .createParamInfo("rank", Integer.class)
        .setDescription("Rank of the factorization (>0).")
        .setHasDefaultValue(10)
        .setAlias(new String[]{"numFactors"})
        .build();

    default Integer getRank() {
        return get(RANK);
    }

    default T setRank(Integer value) {
        return set(RANK, value);
    }

    ParamInfo<Double> LAMBDA = ParamInfoFactory
        .createParamInfo("lambda", Double.class)
        .setDescription("regularization parameter (>= 0).")
        .setHasDefaultValue(0.1)
        .setAlias(new String[]{"regParam"})
        .build();
    ParamInfo<Boolean> NON_NEGATIVE = ParamInfoFactory
        .createParamInfo("nonnegative", Boolean.class)
        .setDescription("Whether enforce the non-negative constraint.")
        .setHasDefaultValue(false)
        .build();
    ParamInfo<Boolean> IMPLICIT_PREFS = ParamInfoFactory
        .createParamInfo("implicitPrefs", Boolean.class)
        .setDescription("Whether to use implicit preference model.")
        .setHasDefaultValue(false)
        .setAlias(new String[]{"implicitPref"})
        .build();
    ParamInfo<Double> ALPHA = ParamInfoFactory
        .createParamInfo("alpha", Double.class)
        .setDescription("The alpha in implicit preference model.")
        .setHasDefaultValue(40.0)
        .build();
    ParamInfo<Integer> NUM_BLOCKS = ParamInfoFactory
        .createParamInfo("numBlocks", Integer.class)
        .setDescription("Number of blocks when doing ALS. This is a performance parameter.")
        .setHasDefaultValue(1)
        .setAlias(new String[]{"numMiniBatches"})
        .build();


    default Double getLambda() {
        return get(LAMBDA);
    }

    default T setLambda(Double value) {
        return set(LAMBDA, value);
    }

    default Boolean getNonnegative() {
        return get(NON_NEGATIVE);
    }

    default T setNonnegative(Boolean value) {
        return set(NON_NEGATIVE, value);
    }

    default Boolean getImplicitPrefs() {
        return get(IMPLICIT_PREFS);
    }

    default T setImplicitPrefs(Boolean value) {
        return set(IMPLICIT_PREFS, value);
    }

    default Double getAlpha() {
        return get(ALPHA);
    }

    default T setAlpha(Double value) {
        return set(ALPHA, value);
    }

    default Integer getNumBlocks() {
        return get(NUM_BLOCKS);
    }

    default T setNumBlocks(Integer value) {
        return set(NUM_BLOCKS, value);
    }
}
