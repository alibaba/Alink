package com.alibaba.alink.params.io;

import com.alibaba.alink.params.io.shared.HasPartitionColsDefaultAsNull;

public interface CsvSinkBatchParams<T> extends
	CsvSinkParams<T>, HasPartitionColsDefaultAsNull<T> {
}
