package com.alibaba.alink.params.dataproc.format;

import com.alibaba.alink.params.io.HasQuoteCharDefaultAsDoubleQuote;
import com.alibaba.alink.params.io.HasSchemaStr;

public interface FromCsvParams<T> extends
	HasCsvCol <T>,
	HasSchemaStr <T>,
	HasCsvFieldDelimiterDefaultAsComma <T>,
	HasQuoteCharDefaultAsDoubleQuote <T> {
}