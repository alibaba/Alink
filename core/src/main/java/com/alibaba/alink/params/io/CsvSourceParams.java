package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.HasHandleInvalid;

public interface CsvSourceParams<T> extends WithParams <T>,
	HasFilePath <T>,
	HasSchemaStr <T>,
	HasFieldDelimiterDvComma <T>,
	HasQuoteCharDefaultAsDoubleQuote <T>,
	HasSkipBlinkLineDefaultAsTrue <T>,
	HasRowDelimiterDvNewline <T>,
	HasIgnoreFirstLine <T>,
	HasLenient <T>,
	HasHandleInvalid <T> {
}
