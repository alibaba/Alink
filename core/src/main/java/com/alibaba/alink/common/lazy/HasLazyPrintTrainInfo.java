package com.alibaba.alink.common.lazy;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface indicating lazy print of training information can be enabled.
 * <p>
 * The enabled status is stored like parameters, while no actual lazy print is realized within this interface.
 * Right now, only {@link com.alibaba.alink.pipeline.Trainer} is supported.
 *
 * @param <T> the Trainer class
 */
public interface HasLazyPrintTrainInfo<T> extends WithParams<T> {

    ParamInfo<Boolean> LAZY_PRINT_TRAIN_INFO_ENABLED = ParamInfoFactory
        .createParamInfo("lazyPrintTrainInfoEnabled", Boolean.class)
        .setDescription("Enable lazyPrint of TrainInfo")
        .setHasDefaultValue(false)
        .build();

    ParamInfo<String> LAZY_PRINT_TRAIN_INFO_TITLE = ParamInfoFactory
        .createParamInfo("lazyPrintTrainInfoTitle", String.class)
        .setDescription("Title of TrainInfo in lazyPrint")
        .setHasDefaultValue(null)
        .build();

    /**
     * Enable lazily printing of training information with a title.
     *
     * @param title title
     * @return the Trainer itself.
     */
    default T enableLazyPrintTrainInfo(String title) {
        this.set(LAZY_PRINT_TRAIN_INFO_ENABLED, true);
        this.set(LAZY_PRINT_TRAIN_INFO_TITLE, title);
        return (T) this;
    }

    /**
     * Enable lazily printing of training information.
     *
     * @return the Trainer itself.
     */
    default T enableLazyPrintTrainInfo() {
        return enableLazyPrintTrainInfo(null);
    }
}
