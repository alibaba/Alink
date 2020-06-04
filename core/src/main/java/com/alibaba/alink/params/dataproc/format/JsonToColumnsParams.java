
package com.alibaba.alink.params.dataproc.format;

public interface JsonToColumnsParams<T> extends
    ToColumnsParams<T>,
    FromJsonParams<T>,
    HasHandleInvalidDefaultAsError<T> {
}
