
package com.alibaba.alink.params.dataproc.format;

public interface VectorToKvParams<T> extends
    ToKvParams<T>,
    FromVectorParams<T>,
    HasHandleInvalidDefaultAsError<T> {
}
