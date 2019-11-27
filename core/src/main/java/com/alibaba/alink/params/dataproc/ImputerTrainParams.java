package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * Parameter of imputer train.
 */
public interface ImputerTrainParams<T>
	extends HasStrategy <T>,
	HasSelectedCols <T> {
}
