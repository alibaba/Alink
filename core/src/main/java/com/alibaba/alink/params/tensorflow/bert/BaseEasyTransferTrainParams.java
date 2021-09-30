package com.alibaba.alink.params.tensorflow.bert;

import com.alibaba.alink.params.dl.HasCheckpointFilePathDefaultAsNull;
import com.alibaba.alink.params.dl.HasIntraOpParallelism;
import com.alibaba.alink.params.dl.HasModelPath;
import com.alibaba.alink.params.dl.HasNumPssDefaultAsNull;
import com.alibaba.alink.params.dl.HasNumWorkersDefaultAsNull;
import com.alibaba.alink.params.dl.HasPythonEnv;
import com.alibaba.alink.params.dl.HasBatchSizeDefaultAs32;
import com.alibaba.alink.params.dl.HasLearningRateDefaultAs0001;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;

public interface BaseEasyTransferTrainParams<T> extends
	HasSelectedColsDefaultAsNull <T>, HasBertModelName <T>,
	HasModelPath <T>, HasNumEpochsDefaultAs001 <T>,
	HasTextCol <T>, HasTextPairCol <T>, HasLabelCol <T>,
	HasMaxSeqLength <T>, HasNumFineTunedLayersDefaultAs1 <T>,
	HasCustomConfigJson <T>,
	HasLearningRateDefaultAs0001 <T>, HasBatchSizeDefaultAs32 <T>,
	HasPythonEnv <T>, HasIntraOpParallelism <T>,
	HasNumWorkersDefaultAsNull <T>, HasNumPssDefaultAsNull <T>,
	HasCheckpointFilePathDefaultAsNull <T> {
}
