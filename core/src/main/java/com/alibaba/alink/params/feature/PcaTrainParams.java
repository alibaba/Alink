package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

/**
 * Trait for parameter PcaTrain.
 */
public interface PcaTrainParams<T> extends
	HasSelectedColsDefaultAsNull <T>,
	HasVectorColDefaultAsNull <T>,
	HasK <T>,
	HasCalculationType <T> {
}
