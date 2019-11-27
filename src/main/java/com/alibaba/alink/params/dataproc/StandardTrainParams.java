package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * Parameter of standard train.
 */
public interface StandardTrainParams<T>
	extends HasSelectedCols <T>,
	HasWithMean <T>,
	HasWithStd <T> {
}
