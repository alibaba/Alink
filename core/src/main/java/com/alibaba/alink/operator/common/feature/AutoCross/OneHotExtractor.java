package com.alibaba.alink.operator.common.feature.AutoCross;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.common.feature.OneHotModelData;
import com.alibaba.alink.operator.common.feature.OneHotModelDataConverter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OneHotExtractor extends RichFlatMapFunction <Integer, Map <Integer, Long>> {
	private Map <Integer, Long> map;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
		if (parallelism != 1) {
			throw new RuntimeException("parallelism of token number extraction must be set as 1.");
		}
		map = new HashMap <>();
		List <Row> modelRows = getRuntimeContext().getBroadcastVariable("model");
		OneHotModelData model = new OneHotModelDataConverter().load(modelRows);
		map = model.modelData.tokenNumber;
		map.keySet().forEach(x -> map.compute(x, (k, v) -> v += 1));//consider null.
	}

	@Override
	public void flatMap(Integer value, Collector <Map <Integer, Long>> out) throws Exception {
		out.collect(map);
	}
}