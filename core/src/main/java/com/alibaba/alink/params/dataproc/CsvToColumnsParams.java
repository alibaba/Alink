package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.io.HasFieldDelimiterDvComma;
import com.alibaba.alink.params.io.HasQuoteCharDefaultAsDoubleQuote;

public interface CsvToColumnsParams<T> extends
    StringToColumnsParams<T>, HasFieldDelimiterDvComma<T>, HasQuoteCharDefaultAsDoubleQuote<T> {
}