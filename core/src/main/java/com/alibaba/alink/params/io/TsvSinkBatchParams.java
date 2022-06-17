package com.alibaba.alink.params.io;

import com.alibaba.alink.params.io.shared.HasPartitionColsDefaultAsNull;

public interface TsvSinkBatchParams<T> extends TsvSinkParams <T>, HasPartitionColsDefaultAsNull <T> {
}
