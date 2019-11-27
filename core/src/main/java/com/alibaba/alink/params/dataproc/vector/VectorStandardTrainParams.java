package com.alibaba.alink.params.dataproc.vector;

import com.alibaba.alink.params.dataproc.HasWithMean;
import com.alibaba.alink.params.dataproc.HasWithStd;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Parameter of standard train for vector data.
 */
public interface VectorStandardTrainParams<T>
	extends HasSelectedCol <T>,
	HasWithMean <T>,
	HasWithStd <T> {
}
