package com.alibaba.alink.params.io;

import com.alibaba.alink.params.shared.HasOverwriteSink;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface CsvSinkParams<T> extends WithParams<T>,
    HasFilePath<T>,
    HasFieldDelimiterDvComma<T>,
    HasRowDelimiterDvNewline<T>,
    HasQuoteCharDefaultAsDoubleQuote<T>,
    HasOverwriteSink<T>,
    HasNumFiles<T> {
}
