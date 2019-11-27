package com.alibaba.alink.params.regression;

import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasLabelCol;

/**
 * parameters of regression train process.
 *
 */
public interface RegressorTrainParams<T> extends
	HasFeatureCols <T>,
	HasLabelCol <T> {
}
