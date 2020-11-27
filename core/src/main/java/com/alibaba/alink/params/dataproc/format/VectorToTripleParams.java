package com.alibaba.alink.params.dataproc.format;

public interface VectorToTripleParams<T> extends
	FromVectorParams <T>,
	HasHandleInvalidDefaultAsError <T> {}