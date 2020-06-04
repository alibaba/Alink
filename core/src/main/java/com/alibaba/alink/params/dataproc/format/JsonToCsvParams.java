
package com.alibaba.alink.params.dataproc.format;

public interface JsonToCsvParams<T> extends
    ToCsvParams<T>,
    FromJsonParams<T>,
    HasHandleInvalidDefaultAsError<T> {
}
