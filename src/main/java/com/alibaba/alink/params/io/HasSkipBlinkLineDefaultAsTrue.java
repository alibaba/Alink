package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasSkipBlinkLineDefaultAsTrue<T> extends WithParams<T> {
    ParamInfo<Boolean> SKIP_BLANK_LINE = ParamInfoFactory
        .createParamInfo("skipBlankLine", Boolean.class)
        .setDescription("skipBlankLine")
        .setHasDefaultValue(true)
        .build();

    default Boolean getSkipBlankLine() {
        return get(SKIP_BLANK_LINE);
    }

    default T setSkipBlankLine(Boolean value) {
        return set(SKIP_BLANK_LINE, value);
    }
}
