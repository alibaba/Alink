package com.alibaba.alink.params.statistics;

import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;

/**
 * Parameter of correlation for table data.
 */
public interface CorrelationParams<T> extends
	HasSelectedColsDefaultAsNull <T>,
	HasMethod <T> {

}
