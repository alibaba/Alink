package com.alibaba.alink.params.dataproc.format;

public interface JsonToTripleParams<T> extends
    FromJsonParams<T>,
    HasHandleInvalidDefaultAsError<T> {}
