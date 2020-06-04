package com.alibaba.alink.params.dataproc.format;

import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface ToKvParams<T> extends
	HasReservedColsDefaultAsNull<T>,
    HasKvCol<T>,
    HasKvColDelimiterDefaultAsComma<T>,
    HasKvValDelimiterDefaultAsColon<T> {
}