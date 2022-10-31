package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.dataproc.format.HasKvValDelimiterDefaultAsColon;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Parameters for {@link com.alibaba.alink.pipeline.dataproc.StringIndexerModel}.
 */
public interface SparseFeatureIndexerTrainParams<T> extends
	HasSelectedCol <T>,
	HasSparseFeatureDelimiterDefaultAsComma <T>,
	HasKvValDelimiterDefaultAsColon <T>,
	HasTopNDefaultAsMinus1 <T>,
	HasMinFrequencyDefaultAsMinus1 <T>,
	HasValueDefaultAsTrue <T> {
}
