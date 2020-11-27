package com.alibaba.alink.params.similarity;

import com.alibaba.alink.params.feature.HasNumHashTables;
import com.alibaba.alink.params.feature.HasNumProjectionsPerTable;
import com.alibaba.alink.params.feature.HasProjectionWidth;
import com.alibaba.alink.params.shared.tree.HasSeed;

/**
 * Params for MinHashTrainer.
 */
public interface VectorLSHParams<T> extends
	HasNumHashTables <T>,
	HasNumProjectionsPerTable <T>,
	HasSeed <T>,
	HasProjectionWidth <T> {
}
