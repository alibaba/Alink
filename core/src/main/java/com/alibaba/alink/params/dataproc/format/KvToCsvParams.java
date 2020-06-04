
package com.alibaba.alink.params.dataproc.format;

public interface KvToCsvParams<T> extends
    ToCsvParams<T>,
    FromKvParams<T>,
    HasHandleInvalidDefaultAsError<T> {
}
