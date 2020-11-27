package com.alibaba.alink.params.dataproc.format;

public interface KvToJsonParams<T> extends
	ToJsonParams <T>,
	FromKvParams <T>,
	HasHandleInvalidDefaultAsError <T> {
}
