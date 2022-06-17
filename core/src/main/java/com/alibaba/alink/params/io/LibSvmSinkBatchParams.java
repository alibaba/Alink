package com.alibaba.alink.params.io;

import com.alibaba.alink.params.io.shared.HasPartitionColsDefaultAsNull;

public interface LibSvmSinkBatchParams<T>
	extends LibSvmSinkParams <T>, HasPartitionColsDefaultAsNull <T> {
}
