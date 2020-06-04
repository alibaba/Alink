package com.alibaba.alink.params.dataproc.format;

public interface TripleToJsonParams<T> extends
    ToJsonParams<T>,
    HasHandleInvalidDefaultAsError<T> {}
