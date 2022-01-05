package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.MinUdaf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MinWithExcludeTest extends AggFunctionTestBase <Object[], Object, MinUdaf.MinMaxData> {

	@Before
	public void init() {
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();

		List <Object[]> data = Arrays.asList(
			new Object[] {9},
			new Object[] {7},
			new Object[] {8},
			new Object[] {6},
			new Object[] {7}
		);

		res.add(data.subList(0, 1));
		res.add(data.subList(0, 2));
		res.add(data.subList(0, 3));
		res.add(data.subList(0, 4));
		res.add(data.subList(0, 5));

		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		return Arrays.asList(null, 9, 7, 7, 6);
	}

	@Override
	protected AggregateFunction <Object, MinUdaf.MinMaxData> getAggregator() {
		return new MinUdaf(true);
	}

	@Override
	protected Class <?> getAccClass() {
		return MinUdaf.MinMaxData.class;
	}
}