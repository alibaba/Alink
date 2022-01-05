package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.BaseSummaryUdaf.SummaryData;
import com.alibaba.alink.common.sql.builtin.agg.CountUdaf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CountWithExcludeLastTest extends AggFunctionTestBase <Object[], Object, SummaryData> {

	@Before
	public void init() {
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();
		List <Object[]> data = Arrays.asList(
			new Object[] {1},
			new Object[] {14});

		res.add(data.subList(0, 1));
		res.add(data.subList(0, 1));
		res.add(data.subList(0, 2));
		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		return Arrays.asList(0L, 0L, 1L);
	}

	@Override
	protected AggregateFunction <Object, SummaryData> getAggregator() {
		return new CountUdaf(true);
	}

	@Override
	protected Class <?> getAccClass() {
		return SummaryData.class;
	}
}
