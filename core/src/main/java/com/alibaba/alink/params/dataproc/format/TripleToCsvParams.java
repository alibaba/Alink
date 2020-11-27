package com.alibaba.alink.params.dataproc.format;

public interface TripleToCsvParams<T> extends
	ToCsvParams <T>,
	HasHandleInvalidDefaultAsError <T> {}
