package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.shared.colname.HasCategoricalCols;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * Params for featureHasher.
 */
public interface FeatureHasherParams<T> extends
	HasSelectedCols <T>,
	HasOutputCol <T>,
	HasReservedCols <T>,
	HasNumFeatures <T>,
	HasCategoricalCols <T> {}
