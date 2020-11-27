package com.alibaba.alink.params.dataproc.format;

public interface KvToColumnsParams<T> extends
	ToColumnsParams <T>,
	FromKvParams <T>,
	HasHandleInvalidDefaultAsError <T> {
}
