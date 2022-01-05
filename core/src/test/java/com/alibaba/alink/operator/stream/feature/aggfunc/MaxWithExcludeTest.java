package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.MaxUdaf;
import com.alibaba.alink.common.sql.builtin.agg.MinUdaf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MaxWithExcludeTest extends AggFunctionTestBase <Object[], Object, MinUdaf.MinMaxData> {

	@Before
	public void init() {
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();
		List <Object[]> data = Arrays.asList(
			new Object[] {1},
			new Object[] {3},
			new Object[] {2},
			new Object[] {4},
			new Object[] {3}
		);

		res.add(new ArrayList <>());
		res.add(data.subList(0, 1));
		res.add(data.subList(0, 2));
		res.add(data.subList(0, 3));
		res.add(data.subList(0, 4));
		res.add(data.subList(0, 5));
		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		return Arrays.asList(null, null, 1, 3, 3, 4);
	}

	@Override
	protected AggregateFunction <Object, MinUdaf.MinMaxData> getAggregator() {
		return new MaxUdaf(true);
	}

	@Override
	protected Class <?> getAccClass() {
		return MinUdaf.MinMaxData.class;
	}
}
