package com.alibaba.alink.params.dataproc.format;

public interface ColumnsToVectorParams<T> extends
	ToVectorParams <T>,
	FromColumnsParams <T>,
	HasHandleInvalidDefaultAsError <T> {
}
