
package com.alibaba.alink.params.dataproc.format;

public interface VectorToJsonParams<T> extends
    ToJsonParams<T>,
    FromVectorParams<T>,
    HasHandleInvalidDefaultAsError<T> {
}
