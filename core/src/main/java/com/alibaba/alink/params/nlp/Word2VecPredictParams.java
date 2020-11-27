package com.alibaba.alink.params.nlp;

import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.shared.colname.HasOutputColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import com.alibaba.alink.params.shared.delimiter.HasWordDelimiter;

public interface Word2VecPredictParams<T> extends
	HasSelectedCol <T>,
	HasReservedColsDefaultAsNull <T>,
	HasOutputColDefaultAsNull <T>,
	HasWordDelimiter <T>,
	HasPredMethod <T>, HasNumThreads <T> {
}
