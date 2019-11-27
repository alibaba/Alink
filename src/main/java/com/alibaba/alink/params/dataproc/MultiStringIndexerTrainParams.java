package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.WithParams;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

public interface MultiStringIndexerTrainParams<T> extends WithParams<T>,
	HasSelectedCols <T>,
    HasStringOrderTypeDefaultAsRandom<T> {
}
