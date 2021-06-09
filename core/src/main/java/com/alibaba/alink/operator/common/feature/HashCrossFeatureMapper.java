package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.mapper.MISOMapper;
import com.alibaba.alink.params.feature.HashCrossFeatureParams;

import static org.apache.flink.shaded.guava18.com.google.common.hash.Hashing.murmur3_32;

public class HashCrossFeatureMapper extends MISOMapper {

	private final int svLength;
	private static final HashFunction HASH = murmur3_32(0);

	public HashCrossFeatureMapper(TableSchema dataSchema,
								  Params params) {
		super(dataSchema, params);
		svLength = params.get(HashCrossFeatureParams.NUM_FEATURES);
	}

	@Override
	protected TypeInformation<?> initOutputColType() {
		return VectorTypes.SPARSE_VECTOR;
	}

	@Override
	protected Object map(Object[] input) throws Exception {
		StringBuilder sbd = new StringBuilder();
		boolean firstData = true;
		for (Object o : input) {
			if (o == null) {
				return new SparseVector(svLength);
			}
			if (!firstData) {
				sbd.append(",");
			} else {
				firstData = false;
			}
			sbd.append(o);
		}
		String s = sbd.toString();
		int hashValue = Math.abs(HASH.hashUnencodedChars(s).asInt());
		hashValue %= svLength;
		return new SparseVector(svLength, new int[] {hashValue}, new double[] {1.0});
	}
}
