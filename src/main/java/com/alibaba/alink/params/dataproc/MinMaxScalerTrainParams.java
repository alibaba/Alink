package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * Parameter of MinMaxScaler train.
 */
public interface MinMaxScalerTrainParams<T> extends
	HasSelectedCols <T>,
	HasMin <T>,
	HasMax <T> {
}
