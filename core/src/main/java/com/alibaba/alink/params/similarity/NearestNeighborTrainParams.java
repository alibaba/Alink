package com.alibaba.alink.params.similarity;

import com.alibaba.alink.params.nlp.HasIdCol;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Base params for NearestNeighborTrain.
 */
public interface NearestNeighborTrainParams<T> extends
	HasIdCol <T>,
	HasSelectedCol <T> {
}
