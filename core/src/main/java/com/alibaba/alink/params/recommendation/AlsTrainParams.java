package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.shared.iter.HasNumIterDefaultAs10;

public interface AlsTrainParams<T> extends
	HasUserCol <T>,
	HasItemCol <T>,
	HasRateCol <T>,
	HasNumIterDefaultAs10 <T> {
	@NameCn("因子数")
	@DescCn("因子数")
	ParamInfo <Integer> RANK = ParamInfoFactory
		.createParamInfo("rank", Integer.class)
		.setDescription("Rank of the factorization (>0).")
		.setHasDefaultValue(10)
		.setAlias(new String[] {"numFactors"})
		.build();

	default Integer getRank() {
		return get(RANK);
	}

	default T setRank(Integer value) {
		return set(RANK, value);
	}

	@NameCn("正则化系数")
	@DescCn("正则化系数")
	ParamInfo <Double> LAMBDA = ParamInfoFactory
		.createParamInfo("lambda", Double.class)
		.setDescription("regularization parameter (>= 0).")
		.setHasDefaultValue(0.1)
		.setAlias(new String[] {"regParam"})
		.build();
	@NameCn("是否约束因子非负")
	@DescCn("是否约束因子非负")
	ParamInfo <Boolean> NON_NEGATIVE = ParamInfoFactory
		.createParamInfo("nonnegative", Boolean.class)
		.setDescription("Whether enforce the non-negative constraint.")
		.setHasDefaultValue(false)
		.build();
	@NameCn("分块数目")
	@DescCn("分块数目")
	ParamInfo <Integer> NUM_BLOCKS = ParamInfoFactory
		.createParamInfo("numBlocks", Integer.class)
		.setDescription("Number of blocks when doing ALS. This is a performance parameter.")
		.setHasDefaultValue(1)
		.setAlias(new String[] {"numMiniBatches"})
		.build();

	default Double getLambda() {
		return get(LAMBDA);
	}

	default T setLambda(Double value) {
		return set(LAMBDA, value);
	}

	default Boolean getNonnegative() {
		return get(NON_NEGATIVE);
	}

	default T setNonnegative(Boolean value) {
		return set(NON_NEGATIVE, value);
	}

	default Integer getNumBlocks() {
		return get(NUM_BLOCKS);
	}

	default T setNumBlocks(Integer value) {
		return set(NUM_BLOCKS, value);
	}
}
