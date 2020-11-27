package com.alibaba.alink.params.dataproc.format;

public interface ColumnsToKvParams<T> extends
	ToKvParams <T>,
	FromColumnsParams <T>,
	HasHandleInvalidDefaultAsError <T> {
}
