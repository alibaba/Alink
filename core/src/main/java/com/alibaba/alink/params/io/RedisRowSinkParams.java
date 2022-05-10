package com.alibaba.alink.params.io;

import com.alibaba.alink.params.dataproc.HasKeyCols;
import com.alibaba.alink.params.dataproc.HasValueCols;

public interface RedisRowSinkParams<T> extends
	RedisParams <T>,
	HasKeyCols <T>,
	HasValueCols <T> {

}
