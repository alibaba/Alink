package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * Param: numProjections.
 *
 * @param <T>
 */
public interface HasNumProjectionsPerTable<T> extends WithParams <T> {

	@NameCn("每个哈希表中的哈希函数个数")
	@DescCn("每个哈希表中的哈希函数个数")
	ParamInfo <Integer> NUM_PROJECTIONS_PER_TABLE = ParamInfoFactory
		.createParamInfo("numProjectionsPerTable", Integer.class)
		.setDescription("The number of hash functions within every hash table")
		.setAlias(new String[] {"numHashBits"})
		.setHasDefaultValue(1)
		.build();

	default Integer getNumProjectionsPerTable() {
		return get(NUM_PROJECTIONS_PER_TABLE);
	}

	default T setNumProjectionsPerTable(Integer value) {
		return set(NUM_PROJECTIONS_PER_TABLE, value);
	}
}
