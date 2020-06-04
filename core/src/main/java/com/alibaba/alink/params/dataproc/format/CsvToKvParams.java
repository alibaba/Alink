
package com.alibaba.alink.params.dataproc.format;

public interface CsvToKvParams<T> extends
    ToKvParams<T>,
    FromCsvParams<T>,
    HasHandleInvalidDefaultAsError<T> {
}
