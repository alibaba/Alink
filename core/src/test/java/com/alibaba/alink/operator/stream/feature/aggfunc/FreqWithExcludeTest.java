package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.DistinctTypeData.FreqData;
import com.alibaba.alink.common.sql.builtin.agg.FreqUdaf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FreqWithExcludeTest extends AggFunctionTestBase <Object[], Long, FreqData> {

	@Before
	public void init() {
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();

		List<Object[]> data = Arrays.asList(new Object[] {1}, new Object[] {1},
			new Object[] {2}, new Object[] {2}, new Object[] {3});

		res.add(data.subList(0, 2));
		res.add(data.subList(0, 3));
		res.add(data.subList(0, 4));
		res.add(data.subList(0, 5));

		return res;
	}

	@Override
	protected List <Long> getExpectedResults() {
		return Arrays.asList(1L, 0L, 1L, 0L);
	}

	@Override
	protected AggregateFunction <Long, FreqData> getAggregator() {
		return new FreqUdaf(true);
	}

	@Override
	protected Class <?> getAccClass() {
		return FreqData.class;
	}
}