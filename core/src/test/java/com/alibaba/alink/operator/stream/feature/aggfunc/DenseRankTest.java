package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.DenseRankUdaf;
import com.alibaba.alink.common.sql.builtin.agg.RankData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DenseRankTest extends AggFunctionTestBase <Long, Long, RankData> {
	@Override
	protected List <List <Long>> getInputValueSets() {
		List <List <Long>> res = new ArrayList <>();

		List <Long> data = Arrays.asList(1L, 1L, 2L, 2L, 4L, 4L);

		res.add(data.subList(0, 1));
		res.add(data.subList(0, 2));
		res.add(data.subList(0, 3));
		res.add(data.subList(0, 4));
		res.add(data.subList(0, 5));
		res.add(data.subList(0, 6));

		return res;
	}

	@Override
	protected List <Long> getExpectedResults() {
		return Arrays.asList(1L, 1L, 2L, 2L, 3L, 3L);
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
