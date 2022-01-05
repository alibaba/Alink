package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.DistinctTypeData.ModeData;
import com.alibaba.alink.common.sql.builtin.agg.ModeUdaf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ModeTest extends AggFunctionTestBase <Object[], Object, ModeData> {

	@Before
	public void init() {
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();

		List <Object[]> data = Arrays.asList(
			new Object[] {2},
			new Object[] {2},
			new Object[] {8},
			new Object[] {8},
			new Object[] {8}
		);

		res.add(data.subList(0, 2));
		res.add(data.subList(0, 3));
		res.add(data.subList(0, 5));

		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		return Arrays.asList(2, 2, 8);
	}

	@Override
	protected AggregateFunction <Object, ModeData> getAggregator() {
		return new ModeUdaf();
	}

	@Override
	protected Class <?> getAccClass() {
		return ModeData.class;
	}
}
