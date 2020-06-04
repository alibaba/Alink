package com.alibaba.alink.params.dataproc.format;

public interface TripleToColumnsParams<T> extends
    ToColumnsParams<T>,
    HasHandleInvalidDefaultAsError<T> {}
