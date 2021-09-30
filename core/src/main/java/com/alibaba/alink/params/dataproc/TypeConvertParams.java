package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;

public interface TypeConvertParams<T>
	extends WithParams <T>,
	HasSelectedColsDefaultAsNull <T>,
	HasTargetType <T> {
}
