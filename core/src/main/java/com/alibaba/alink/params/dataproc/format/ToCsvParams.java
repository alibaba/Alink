package com.alibaba.alink.params.dataproc.format;

import com.alibaba.alink.params.io.HasQuoteCharDefaultAsDoubleQuote;
import com.alibaba.alink.params.io.HasSchemaStr;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface ToCsvParams<T> extends
	HasReservedColsDefaultAsNull<T>,
	HasCsvCol <T>,
	HasSchemaStr <T>,
    HasCsvFieldDelimiterDefaultAsComma<T>,
	HasQuoteCharDefaultAsDoubleQuote <T> {
}