
package com.alibaba.alink.params.dataproc.format;

public interface KvToVectorParams<T> extends
    ToVectorParams<T>,
    FromKvParams<T>,
    HasHandleInvalidDefaultAsError<T> {
}
