package com.alibaba.alink.params.tensorflow.kerasequential;

import com.alibaba.alink.params.dl.HasCheckpointFilePathDefaultAsNull;
import com.alibaba.alink.params.dl.HasIntraOpParallelism;
import com.alibaba.alink.params.dl.HasNumPssDefaultAsNull;
import com.alibaba.alink.params.dl.HasNumWorkersDefaultAsNull;
import com.alibaba.alink.params.dl.HasPythonEnv;
import com.alibaba.alink.params.dl.HasBatchSizeDefaultAs128;
import com.alibaba.alink.params.dl.HasLearningRateDefaultAs0001;
import com.alibaba.alink.params.dl.HasNumEpochsDefaultAs10;
import com.alibaba.alink.params.image.HasTensorCol;
import com.alibaba.alink.params.shared.colname.HasLabelCol;

public interface BaseKerasSequentialTrainParams<T> extends
	HasTensorCol <T>, HasLabelCol <T>,
	HasLayers <T>, HasOptimizer <T>,
	HasLearningRateDefaultAs0001 <T>,
	HasNumEpochsDefaultAs10 <T>, HasBatchSizeDefaultAs128 <T>, HasCheckpointFilePathDefaultAsNull <T>,
	HasPythonEnv <T>, HasIntraOpParallelism <T>,
	HasNumWorkersDefaultAsNull <T>, HasNumPssDefaultAsNull <T> {
}
