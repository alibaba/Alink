package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.LastValueTypeData.SumLastData;
import com.alibaba.alink.common.sql.builtin.agg.SumLastUdaf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SumLastTest extends AggFunctionTestBase <Object[], Object, SumLastData> {

	@Before
	public void init() {
		useRetract = false;
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();
		List <Object[]> data = Arrays.asList(
			new Object[] {1, 1L, 0.1},
			new Object[] {1, 2L, 0.1},
			new Object[] {3, 3L, 0.1},
			new Object[] {3, 4L, 0.1},
			new Object[] {4, 5L, 0.1},
			new Object[] {4, 6L, 0.1},
			new Object[] {2, 7L, 0.1},
			new Object[] {2, 8L, 0.1}
		);

		res.add(data.subList(0, 1));
		res.add(data.subList(0, 2));
		res.add(data.subList(0, 3));
		res.add(data.subList(0, 4));
		res.add(data.subList(0, 5));
		res.add(data.subList(0, 6));
		res.add(data.subList(0, 7));
		res.add(data.subList(0, 8));

		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		return Arrays.asList(1, 1, 3, 3, 4, 4, 2, 2);
	}

	@Override
	protected AggregateFunction <Object, SumLastData> getAggregator() {
		return new SumLastUdaf(3, 0.5);
	}

	@Override
	protected Class <?> getAccClass() {
		return SumLastData.class;
	}
}
