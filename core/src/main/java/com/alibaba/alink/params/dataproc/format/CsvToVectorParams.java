
package com.alibaba.alink.params.dataproc.format;

public interface CsvToVectorParams<T> extends
    ToVectorParams<T>,
    FromCsvParams<T>,
    HasHandleInvalidDefaultAsError<T> {
}
