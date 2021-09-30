package com.alibaba.alink.params.timeseries;

import com.alibaba.alink.params.dl.HasBatchSizeDefaultAs128;
import com.alibaba.alink.params.dl.HasCheckpointFilePath;
import com.alibaba.alink.params.dl.HasIntraOpParallelism;
import com.alibaba.alink.params.dl.HasLearningRateDefaultAs0001;
import com.alibaba.alink.params.dl.HasNumEpochsDefaultAs10;
import com.alibaba.alink.params.dl.HasNumPssDefaultAsNull;
import com.alibaba.alink.params.dl.HasNumWorkersDefaultAsNull;
import com.alibaba.alink.params.dl.HasPythonEnv;
import com.alibaba.alink.params.nlp.HasWindow;
import com.alibaba.alink.params.shared.HasTimeCol;
import com.alibaba.alink.params.shared.colname.HasSelectedColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

public interface LSTNetTrainParams<T> extends
	HasTimeCol <T>,
	HasSelectedColDefaultAsNull <T>,
	HasVectorColDefaultAsNull <T>,
	HasWindow <T>,
	HasHorizon <T>,
	HasLearningRateDefaultAs0001 <T>,
	HasNumEpochsDefaultAs10 <T>, HasBatchSizeDefaultAs128 <T>, HasCheckpointFilePath <T>,
	HasPythonEnv <T>, HasIntraOpParallelism <T>,
	HasNumWorkersDefaultAsNull <T>, HasNumPssDefaultAsNull <T> {
}
