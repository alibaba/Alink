package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.shared.clustering.HasKMeansDistanceType;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs10;

/**
 * Params for BisectingKMeansTrain.
 */
public interface BisectingKMeansTrainParams<T> extends WithParams <T>,
	HasKMeansDistanceType <T>,
	HasVectorCol <T>,
	HasMaxIterDefaultAs10 <T>,
	HasRandomSeed <T> {

	@NameCn("最小可分裂的聚类大小")
	@DescCn("最小可分裂的聚类大小")
	ParamInfo <Integer> MIN_DIVISIBLE_CLUSTER_SIZE = ParamInfoFactory
		.createParamInfo("minDivisibleClusterSize", Integer.class)
		.setDescription("Minimum divisible cluster size")
		.setHasDefaultValue(1)
		.build();
	@NameCn("聚类中心点数目")
	@DescCn("聚类中心点数目")
	ParamInfo <Integer> K = ParamInfoFactory
		.createParamInfo("k", Integer.class)
		.setDescription("Number of clusters.")
		.setHasDefaultValue(4)
		.build();

	default Integer getMinDivisibleClusterSize() {
		return get(MIN_DIVISIBLE_CLUSTER_SIZE);
	}

	default T setMinDivisibleClusterSize(Integer value) {
		return set(MIN_DIVISIBLE_CLUSTER_SIZE, value);
	}

	default Integer getK() {
		return get(K);
	}

	default T setK(Integer value) {
		return set(K, value);
	}

}
