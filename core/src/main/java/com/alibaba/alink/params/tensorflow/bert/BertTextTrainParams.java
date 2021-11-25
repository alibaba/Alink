package com.alibaba.alink.params.tensorflow.bert;

import com.alibaba.alink.params.dl.HasBatchSizeDefaultAs32;
import com.alibaba.alink.params.dl.HasCheckpointFilePathDefaultAsNull;
import com.alibaba.alink.params.dl.HasIntraOpParallelism;
import com.alibaba.alink.params.dl.HasLearningRateDefaultAs0001;
import com.alibaba.alink.params.dl.HasNumPssDefaultAsNull;
import com.alibaba.alink.params.dl.HasNumWorkersDefaultAsNull;
import com.alibaba.alink.params.dl.HasPythonEnv;
import com.alibaba.alink.params.shared.colname.HasLabelCol;

public interface BertTextTrainParams<T> extends
	HasTextCol <T>, HasLabelCol <T>, HasMaxSeqLength <T>,
	HasBertModelName <T>, HasNumFineTunedLayersDefaultAs1 <T>,
	HasCustomConfigJson <T>,
	HasCheckpointFilePathDefaultAsNull <T>,
	HasNumEpochsDefaultAs001 <T>, HasLearningRateDefaultAs0001 <T>, HasBatchSizeDefaultAs32 <T>,
	HasIntraOpParallelism <T>,
	HasNumWorkersDefaultAsNull <T>, HasNumPssDefaultAsNull <T>,
	HasPythonEnv <T> {
}
