package com.alibaba.alink.operator.common.similarity.modeldata;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.common.similarity.lsh.BaseLSH;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;

public class LocalLSHModelData extends LocalHashModelData {
	private static final long serialVersionUID = 1431580949382378771L;

	private final Map <Object, Vector> data;
	private final BaseLSH lsh;

	public LocalLSHModelData(int numHashTables, Map <Long, Collection <Object>> indexMap,
							 Map <Object, Vector> data, BaseLSH lsh) {
		super(numHashTables, indexMap);
		this.data = data;
		this.lsh = lsh;
		comparator = Comparator.comparingDouble(o -> -o.f0);
	}

	@Override
	protected Tuple2 <Object, int[]> getSampleAndHashValues(Object input) {
		Vector v = VectorUtil.getVector(input);
		int[] hashValue = lsh.hashFunction(v);
		return Tuple2.of(v, hashValue);
	}

	@Override
	protected Double computeHashDistance(Object inputSample, Object sample) {
		return lsh.keyDistance((Vector) inputSample, data.get(sample));
	}
}
