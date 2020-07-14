package com.alibaba.alink.common.lazy;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface indicating lazy print of model information can be enabled.
 * <p>
 * The enabled status is stored like parameters, while no actual lazy print is realized within this interface.
 * Right now, only {@link com.alibaba.alink.pipeline.Trainer} is supported.
 *
 * @param <T> the Trainer class
 */
public interface HasLazyPrintModelInfo<T> extends WithParams<T> {

    ParamInfo<Boolean> LAZY_PRINT_MODEL_INFO_ENABLED = ParamInfoFactory
        .createParamInfo("lazyPrintModelInfoEnabled", Boolean.class)
        .setDescription("Enable lazyPrint of ModelInfo")
        .setHasDefaultValue(false)
        .build();

    ParamInfo<String> LAZY_PRINT_MODEL_INFO_TITLE = ParamInfoFactory
        .createParamInfo("lazyPrintModelInfoTitle", String.class)
        .setDescription("Title of ModelInfo in lazyPrint")
        .setHasDefaultValue(null)
        .build();

    /**
     * Enable lazily printing of model information with a title.
     *
     * @param title title
     * @return the Trainer itself.
     */
    default T enableLazyPrintModelInfo(String title) {
        this.set(LAZY_PRINT_MODEL_INFO_ENABLED, true);
        this.set(LAZY_PRINT_MODEL_INFO_TITLE, title);
        return (T) this;
    }

    /**
     * Enable lazily printing of model information.
     *
     * @return the Trainer itself.
     */
    default T enableLazyPrintModelInfo() {
        return enableLazyPrintModelInfo(null);
    }
}
