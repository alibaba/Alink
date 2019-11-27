package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import com.alibaba.alink.params.shared.tree.HasSeed;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * Params for MinHashTrainer.
 */
public interface BaseLSHTrainParams<T> extends
	HasNumHashTables <T>,
	HasSelectedCol <T>,
    HasNumProjectionsPerTable<T>,
	HasSeed <T> {
}
