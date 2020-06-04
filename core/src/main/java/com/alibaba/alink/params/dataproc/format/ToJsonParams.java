package com.alibaba.alink.params.dataproc.format;

import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface ToJsonParams<T> extends
    HasReservedColsDefaultAsNull<T>,
    HasJsonCol<T> {
}