package com.alibaba.alink.params.statistics;

import org.apache.flink.ml.api.misc.param.WithParams;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Trait for parameter VectorChiSquareTest.
 */
public interface VectorChiSquareTestParams<T> extends WithParams<T>,
	HasLabelCol <T>,
	HasSelectedCol <T> {
}
