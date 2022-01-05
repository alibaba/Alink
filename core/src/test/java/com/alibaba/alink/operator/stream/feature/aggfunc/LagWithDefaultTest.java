package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.LagUdaf;
import com.alibaba.alink.common.sql.builtin.agg.LastValueTypeData.LagData;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LagWithDefaultTest extends AggFunctionTestBase <Object[], Object, LagData> {

	@Before
	public void init() {
		useRetract = false;
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();

		List <Object[]> data = Arrays.asList(new Object[] {1, 2, 33}, new Object[] {1, 2, 33},
			new Object[] {3, 2, 33}, new Object[] {3, 2, 33}, new Object[] {4, 2, 33},
			new Object[] {4, 2, 33}, new Object[] {2, 2, 33}, new Object[] {2, 2, 33});

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
		return Arrays.asList(33, 33, 1, 1, 3, 3, 4, 4);
	}

	@Override
	protected AggregateFunction <Object, LagData> getAggregator() {
		return new LagUdaf();
	}

	@Override
	protected Class <?> getAccClass() {
		return LagData.class;
	}
}
