package com.alibaba.alink.params.tensorflow.bert;

import com.alibaba.alink.params.dl.HasIntraOpParallelism;
import com.alibaba.alink.params.dl.HasModelPath;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

public interface BertTextEmbeddingParams<T> extends
	HasSelectedCol <T>, HasOutputCol <T>, HasMaxSeqLength <T>, HasDoLowerCaseDefaultAsNull <T>, HasLayer <T>,
	HasBertModelName <T>, HasModelPath <T>,
	HasReservedColsDefaultAsNull <T>,
	HasIntraOpParallelism <T> {
}
