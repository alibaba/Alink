package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.clustering.HasEpsilonDv00001;
import com.alibaba.alink.params.shared.clustering.HasKDefaultAs2;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;

/**
 * Parameters for Gaussian Mixture Model training.
 *
 * @param <T> The class that implement this interface.
 */
public interface GmmTrainParams<T> extends WithParams <T>,
	HasVectorCol <T>,
	HasKDefaultAs2 <T>,
	HasMaxIterDefaultAs100 <T>,
	HasEpsilonDv00001 <T>,
	HasRandomSeed <T> {
}
