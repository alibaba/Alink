
package com.alibaba.alink.params.dataproc.format;

public interface ColumnsToJsonParams<T> extends
    ToJsonParams<T>,
    FromColumnsParams<T>,
    HasHandleInvalidDefaultAsError<T> {
}
