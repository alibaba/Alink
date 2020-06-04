package com.alibaba.alink.params.dataproc.format;

import com.alibaba.alink.params.io.HasSchemaStr;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface ToColumnsParams<T> extends
	HasReservedColsDefaultAsNull<T>,
	HasSchemaStr <T> {
}