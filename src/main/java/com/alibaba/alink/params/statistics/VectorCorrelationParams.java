package com.alibaba.alink.params.statistics;

import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Parameter of correlation for vector data.
 */
public interface VectorCorrelationParams<T> extends
	HasSelectedCol <T>,
	HasMethod <T> {
}
