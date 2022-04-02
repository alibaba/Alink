
package com.alibaba.alink.params.tensorflow.savedmodel;

import com.alibaba.alink.params.dl.HasIntraOpParallelism;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;

public interface BaseTFSavedModelPredictParams<T> extends HasReservedColsDefaultAsNull <T>,
	HasGraphDefTag <T>, HasSignatureDefKey <T>,
	HasInputSignatureDefs <T>, HasOutputSignatureDefs <T>,
	HasSelectedColsDefaultAsNull <T>, HasOutputSchemaStr <T>,
	HasIntraOpParallelism <T> {
}
