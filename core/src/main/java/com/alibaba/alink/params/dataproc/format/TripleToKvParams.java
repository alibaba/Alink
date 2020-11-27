package com.alibaba.alink.params.dataproc.format;

public interface TripleToKvParams<T> extends
	ToKvParams <T>,
	HasHandleInvalidDefaultAsError <T> {}
