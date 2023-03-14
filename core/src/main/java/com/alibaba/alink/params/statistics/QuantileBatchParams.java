package com.alibaba.alink.params.statistics;

import com.alibaba.alink.params.shared.colname.HasSelectedCol;

public interface QuantileBatchParams<T> extends
	HasSelectedCol <T>,
	HasQuantileNum <T>,
	HasRoundMode <T> {
}
