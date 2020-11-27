package com.alibaba.alink.params.dataproc.format;

public interface ColumnsToCsvParams<T> extends
	ToCsvParams <T>,
	FromColumnsParams <T>,
	HasHandleInvalidDefaultAsError <T> {
}
