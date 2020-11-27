package com.alibaba.alink.params.dataproc.format;

public interface VectorToCsvParams<T> extends
	ToCsvParams <T>,
	FromVectorParams <T>,
	HasHandleInvalidDefaultAsError <T> {
}
