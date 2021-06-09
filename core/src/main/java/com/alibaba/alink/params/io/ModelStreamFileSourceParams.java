package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.io.shared.HasScanIntervalDefaultAs10;
import com.alibaba.alink.params.io.shared.HasStartTimeUseTimestampDefaultAsNull;

public interface ModelStreamFileSourceParams<T> extends WithParams <T>,
	HasFilePath <T>,
	HasStartTimeUseTimestampDefaultAsNull <T>,
	HasSchemaStrDefaultAsNull <T>,
	HasScanIntervalDefaultAs10 <T> {
}
