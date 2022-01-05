package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.RankData;
import com.alibaba.alink.common.sql.builtin.agg.RowNumberUdaf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RowNumberTest extends AggFunctionTestBase <Long, Long, RankData> {
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
		return Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L);
	}

	@Override
	protected AggregateFunction <Long, RankData> getAggregator() {
		return new RowNumberUdaf();
	}

	@Override
	protected Class <?> getAccClass() {
		return RankData.class;
	}
}
