package com.alibaba.alink.params.io;

import com.alibaba.alink.params.io.shared.HasPartitionColsDefaultAsNull;

public interface TextSinkBatchParams<T> extends TextSinkParams <T>, HasPartitionColsDefaultAsNull <T> {
}
