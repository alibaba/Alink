package com.alibaba.alink.operator.common.statistics.statistics;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.metadata.def.shaded.com.google.protobuf.InvalidProtocolBufferException;
import com.alibaba.alink.metadata.def.v0.DatasetFeatureStatisticsList;

import java.util.Base64;
import java.util.Collections;

public class FullStatsConverter
	extends SimpleModelDataConverter <FullStats, FullStats> {

	public FullStatsConverter() {
	}

	@Override
	public Tuple2 <Params, Iterable <String>> serializeModel(FullStats fullStats) {
		byte[] bytes = fullStats.getDatasetFeatureStatisticsList().toByteArray();
		return Tuple2.of(new Params(), Collections.singletonList(Base64.getEncoder().encodeToString(bytes)));
	}

	@Override
	public FullStats deserializeModel(Params meta, Iterable <String> data) {
		for (String s : data) {
			byte[] bytes = Base64.getDecoder().decode(s);
			try {
				return new FullStats(DatasetFeatureStatisticsList.parseFrom(bytes));
			} catch (InvalidProtocolBufferException e) {
				throw new RuntimeException("Failed to parse from base64 string: " + s, e);
			}
		}
		throw new IllegalArgumentException("No data for deserialize.");
	}
}
