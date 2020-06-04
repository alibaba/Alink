
package com.alibaba.alink.params.dataproc.format;

public interface JsonToVectorParams<T> extends
    ToVectorParams<T>,
    FromJsonParams<T>,
    HasHandleInvalidDefaultAsError<T> {
}
