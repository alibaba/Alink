
package com.alibaba.alink.params.dl;

import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.params.tensorflow.savedmodel.HasOutputSchemaStr;

public interface TorchModelPredictParams<T> extends
	HasSelectedCols <T>, HasReservedColsDefaultAsNull <T>, HasModelPath <T>, HasOutputSchemaStr <T>,
	HasIntraOpParallelism <T> {
}
