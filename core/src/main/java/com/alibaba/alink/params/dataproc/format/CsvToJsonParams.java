
package com.alibaba.alink.params.dataproc.format;

public interface CsvToJsonParams<T> extends
    ToJsonParams<T>,
    FromCsvParams<T>,
    HasHandleInvalidDefaultAsError<T> {
}
