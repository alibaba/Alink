package com.alibaba.alink.params.io;

import com.alibaba.alink.params.io.shared.HasPartitionColsDefaultAsNull;

public interface AkSinkBatchParams<T> extends AkSinkParams <T>,
	HasPartitionColsDefaultAsNull <T> {
}
