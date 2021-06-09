package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.mapper.MapperParams;
import com.alibaba.alink.params.shared.colname.HasCategoricalCols;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * Params for featureHasher.
 */
public interface FeatureHasherParams<T> extends
	MapperParams <T>,
	HasSelectedCols <T>,
	HasOutputCol <T>,
	HasReservedColsDefaultAsNull <T>,
	HasNumFeatures <T>,
	HasCategoricalCols <T> {}
