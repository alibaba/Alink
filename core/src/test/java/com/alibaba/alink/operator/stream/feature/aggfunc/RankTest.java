package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.RankData;
import com.alibaba.alink.common.sql.builtin.agg.RankUdaf;

import java.util.ArrayList;
import java.util.List;

public class RankTest extends AggFunctionTestBase <Long, Long, RankData> {
	@Override
	protected List <List <Long>> getInputValueSets() {
		List <List <Long>> res = new ArrayList <>();
		ArrayList <Long> data1 = new ArrayList <>();
		data1.add(1L);
		res.add((List <Long>) data1.clone());
		data1.add(1L);
		res.add((List <Long>) data1.clone());
		data1.add(3L);
		res.add((List <Long>) data1.clone());
		data1.add(3L);
		res.add((List <Long>) data1.clone());
		data1.add(5L);
		res.add((List <Long>) data1.clone());
		data1.add(5L);
		res.add((List <Long>) data1.clone());
		data1.add(7L);
		res.add((List <Long>) data1.clone());
		data1.add(7L);
		res.add((List <Long>) data1.clone());
		return res;
	}

	@Override
	protected List <Long> getExpectedResults() {
		List <Long> res = new ArrayList <>();
		res.add(1L);
		res.add(1L);
		res.add(3L);
		res.add(3L);
		res.add(5L);
		res.add(5L);
		res.add(7L);
		res.add(7L);
		return res;
	}

	@Override
	protected AggregateFunction <Long, RankData> getAggregator() {
		return new RankUdaf();
	}

	@Override
	protected Class <?> getAccClass() {
		return RankData.class;
	}
}
