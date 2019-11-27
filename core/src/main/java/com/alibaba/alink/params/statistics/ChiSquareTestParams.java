package com.alibaba.alink.params.statistics;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * Trait for parameter ChiSquareTest.
 */
public interface ChiSquareTestParams<T> extends WithParams<T>,
	HasLabelCol <T>,
	HasSelectedCols <T> {
}
