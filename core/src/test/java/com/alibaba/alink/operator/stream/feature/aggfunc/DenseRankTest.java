package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.DenseRankUdaf;
import com.alibaba.alink.common.sql.builtin.agg.RankData;

import java.util.ArrayList;
import java.util.List;

public class DenseRankTest extends AggFunctionTestBase <Long, Long, RankData> {
	@Override
	protected List <List <Long>> getInputValueSets() {
		List <List <Long>> res = new ArrayList <>();
		ArrayList <Long> data1 = new ArrayList <>();
		data1.add(1L);
		res.add((List <Long>) data1.clone());
		data1.add(1L);
		res.add((List <Long>) data1.clone());
		data1.add(2L);
		res.add((List <Long>) data1.clone());
		data1.add(2L);
		res.add((List <Long>) data1.clone());
		data1.add(4L);
		res.add((List <Long>) data1.clone());
		data1.add(4L);
		res.add((List <Long>) data1.clone());
		return res;
	}

	@Override
	protected List <Long> getExpectedResults() {
		List <Long> res = new ArrayList <>();
		res.add(1L);
		res.add(1L);
		res.add(2L);
		res.add(2L);
		res.add(3L);
		res.add(3L);
		return res;
	}

	@Override
	protected AggregateFunction <Long, RankData> getAggregator() {
		return new DenseRankUdaf();
	}

	@Override
	protected Class <?> getAccClass() {
		return RankData.class;
	}
}
