
package com.alibaba.alink.params.dataproc.format;

public interface CsvToColumnsParams<T> extends
    ToColumnsParams<T>,
    FromCsvParams<T>,
    HasHandleInvalidDefaultAsError<T> {
}
