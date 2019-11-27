package com.alibaba.alink.params.similarity;

import com.alibaba.alink.params.feature.BaseLSHTrainParams;
import com.alibaba.alink.params.feature.HasProjectionWidth;
import com.alibaba.alink.params.shared.clustering.HasDistanceType;
import com.alibaba.alink.params.shared.colname.HasOutputCol;

/**
 * Base params for LSH algorithm.
 */
public interface BaseJoinTopNLSHParams<T> extends
	HasLeftCol<T>,
    HasRightCol<T>,
	HasDistanceType<T>,
	HasOutputCol <T>,
    HasLeftIdCol<T>,
	HasRightIdCol<T>,
	BaseLSHTrainParams<T>,
	HasProjectionWidth<T> {
}
