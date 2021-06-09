package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

/**
 * Trait for parameter PcaPredict.
 */
public interface PcaPredictParams<T> extends
	ModelMapperParams <T>,
	HasReservedColsDefaultAsNull <T>,
	HasPredictionCol <T>,
	HasVectorColDefaultAsNull <T> {
}
