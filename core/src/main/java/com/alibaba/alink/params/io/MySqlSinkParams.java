package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.WithParams;
import com.alibaba.alink.params.io.shared_params.HasOutputTableName;

public interface MySqlSinkParams<T> extends WithParams<T>,
	MySqlDBParams <T>,
	HasOutputTableName <T> {
}
