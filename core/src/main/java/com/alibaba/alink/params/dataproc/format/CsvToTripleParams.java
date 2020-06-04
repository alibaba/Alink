package com.alibaba.alink.params.dataproc.format;

public interface CsvToTripleParams<T> extends
    FromCsvParams<T>,
    HasHandleInvalidDefaultAsError<T> {
}
