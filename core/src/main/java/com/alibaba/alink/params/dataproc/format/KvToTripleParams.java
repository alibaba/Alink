package com.alibaba.alink.params.dataproc.format;

public interface KvToTripleParams<T> extends
    FromKvParams<T>,
    HasHandleInvalidDefaultAsError<T> {}
