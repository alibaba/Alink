package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasQuoteCharDefaultAsDoubleQuote<T> extends WithParams<T> {
    ParamInfo<Character> QUOTE_CHAR = ParamInfoFactory
        .createParamInfo("quoteChar", Character.class)
        .setDescription("quote char")
        .setHasDefaultValue('"')
        .build();

    default Character getQuoteChar() {
        return get(QUOTE_CHAR);
    }

    default T setQuoteChar(Character value) {
        return set(QUOTE_CHAR, value);
    }
}
