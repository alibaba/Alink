package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasWeightCol;

/**
 * Params for WeightSampleBatchOp.
 */
public interface WeightSampleParams<T> extends
	HasWeightCol <T>,
	SampleParams <T> {
}
