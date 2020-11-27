package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.colname.HasOutputColDefaultAsNull;

public interface NumSeqSourceParams<T>
	extends WithParams <T>,
	HasFrom <T>,
	HasTo <T>,
	HasOutputColDefaultAsNull <T> {
}
