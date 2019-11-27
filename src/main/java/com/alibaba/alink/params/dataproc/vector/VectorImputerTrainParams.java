package com.alibaba.alink.params.dataproc.vector;

import org.apache.flink.ml.api.misc.param.WithParams;
import com.alibaba.alink.params.dataproc.HasStrategy;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Parameter of imputer train for vector data.
 */
public interface VectorImputerTrainParams<T>
	extends WithParams<T>,
	HasStrategy <T>,
	HasSelectedCol <T> {
}
