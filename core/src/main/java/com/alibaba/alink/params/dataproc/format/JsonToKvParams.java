
package com.alibaba.alink.params.dataproc.format;

public interface JsonToKvParams<T> extends
    ToKvParams<T>,
    FromJsonParams<T>,
    HasHandleInvalidDefaultAsError<T> {
}
