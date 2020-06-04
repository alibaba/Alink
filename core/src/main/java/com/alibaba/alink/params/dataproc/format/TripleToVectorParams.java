package com.alibaba.alink.params.dataproc.format;

public interface TripleToVectorParams<T> extends
    ToVectorParams<T>,
    HasHandleInvalidDefaultAsError<T> {}
