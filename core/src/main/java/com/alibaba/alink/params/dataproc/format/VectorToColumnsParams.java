
package com.alibaba.alink.params.dataproc.format;

public interface VectorToColumnsParams<T> extends
    ToColumnsParams<T>,
    FromVectorParams<T>,
    HasHandleInvalidDefaultAsError<T> {
}
