package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasWeightCol;
import com.alibaba.alink.params.validators.RangeValidator;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * Params for WeightSampleBatchOp.
 */
public interface WeightSampleParams<T> extends
    HasWeightCol<T>,
    SampleParams<T>{
}
