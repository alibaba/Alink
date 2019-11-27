package com.alibaba.alink.params.nlp;

import com.alibaba.alink.params.shared.colname.HasOutputColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import com.alibaba.alink.params.shared.delimiter.HasWordDelimiter;

public interface Word2VecPredictParams<T> extends
	HasSelectedCol <T>,
	HasReservedCols <T>,
	HasOutputColDefaultAsNull <T>,
	HasWordDelimiter <T>,
	HasPredMethod <T> {
}
