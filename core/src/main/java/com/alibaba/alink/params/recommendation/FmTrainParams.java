package com.alibaba.alink.params.recommendation;

import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import com.alibaba.alink.params.shared.linear.HasEpsilonDv0000001;
import com.alibaba.alink.params.shared.linear.HasWithIntercept;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * parameters of fm trainer.
 */
public interface FmTrainParams<T> extends
    HasLabelCol<T>,
    HasVectorColDefaultAsNull<T>,
    HasWeightColDefaultAsNull<T>,
    HasEpsilonDv0000001<T>,
    HasFeatureColsDefaultAsNull<T>,
    HasWithIntercept<T> {

    ParamInfo<Boolean> WITH_LINEAR_ITEM = ParamInfoFactory
        .createParamInfo("hasLinearItem", Boolean.class)
        .setDescription("has linear item.")
        .setHasDefaultValue(true)
        .build();
    default Boolean getWithLinearItem() {
        return get(WITH_LINEAR_ITEM);
    }
    default T setWithLinearItem(Boolean value) {
        return set(WITH_LINEAR_ITEM, value);
    }

    ParamInfo<Integer> NUM_FACTOR = ParamInfoFactory
        .createParamInfo("numFactor", Integer.class)
        .setDescription("number of factor")
        .setHasDefaultValue(10)
        .build();
    default Integer getNumFactor() {
        return get(NUM_FACTOR);
    }
    default T setNumFactor(Integer value) {
        return set(NUM_FACTOR, value);
    }

    ParamInfo<Double> LAMBDA_0 = ParamInfoFactory
        .createParamInfo("lambda_0", Double.class)
        .setDescription("lambda_0")
        .setHasDefaultValue(0.0)
        .build();
    default Double getLambda0() {
        return get(LAMBDA_0);
    }
    default T setLambda0(Double value) {
        return set(LAMBDA_0, value);
    }

    ParamInfo<Double> LAMBDA_1 = ParamInfoFactory
        .createParamInfo("lambda_1", Double.class)
        .setDescription("lambda_1")
        .setHasDefaultValue(0.0)
        .build();
    default Double getLambda1() {
        return get(LAMBDA_1);
    }
    default T setLambda1(Double value) {
        return set(LAMBDA_1, value);
    }

    ParamInfo<Double> LAMBDA_2 = ParamInfoFactory
        .createParamInfo("lambda_2", Double.class)
        .setDescription("lambda_2")
        .setHasDefaultValue(0.0)
        .build();
    default Double getLambda2() {
        return get(LAMBDA_2);
    }
    default T setLambda2(Double value) {
        return set(LAMBDA_2, value);
    }

    ParamInfo<Double> INIT_STDEV = ParamInfoFactory
        .createParamInfo("initStdev", Double.class)
        .setDescription("init stdev")
        .setHasDefaultValue(0.05)
        .build();
    default Double getInitStdev() {
        return get(INIT_STDEV);
    }
    default T setInitStdev(Double value) {
        return set(INIT_STDEV, value);
    }

    ParamInfo<Integer> MINIBATCH_SIZE = ParamInfoFactory
        .createParamInfo("minibatchSize", Integer.class)
        .setDescription("mini-batch size")
        .setHasDefaultValue(-1)
        .build();
    default Integer getBatchSize() {
        return get(MINIBATCH_SIZE);
    }
    default T setBatchSize(Integer value) {
        return set(MINIBATCH_SIZE, value);
    }

    ParamInfo<Double> LEARN_RATE = ParamInfoFactory
        .createParamInfo("learnRate", Double.class)
        .setDescription("learn rate")
        .setHasDefaultValue(0.01)
        .build();
    default Double getLearnRate() {
        return get(LEARN_RATE);
    }
    default T setLearnRate(Double value) {
        return set(LEARN_RATE, value);
    }

    ParamInfo<Integer> NUM_EPOCHS = ParamInfoFactory
        .createParamInfo("numEpochs", Integer.class)
        .setDescription("num epochs")
        .setHasDefaultValue(10)
        .setAlias(new String[]{"numIter"})
        .build();

    default Integer getNumEpochs() {
        return get(NUM_EPOCHS);
    }

    default T setNumEpochs(Integer value) {
        return set(NUM_EPOCHS, value);
    }
}
