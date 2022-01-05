package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.DistinctTypeData;
import com.alibaba.alink.common.sql.builtin.agg.ModeUdaf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ModeWithExcludeTest extends AggFunctionTestBase <Object[], Object, DistinctTypeData.ModeData> {

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
			new Object[] {8},
			new Object[] {8}
		);

		res.add(data.subList(0, 1));
		res.add(data.subList(0, 2));
		res.add(data.subList(0, 3));
		res.add(data.subList(0, 6));

		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		return Arrays.asList(null, 2, 2, 8);
	}

	@Override
	protected AggregateFunction <Object, DistinctTypeData.ModeData> getAggregator() {
		return new ModeUdaf(true);
	}

	@Override
	protected Class <?> getAccClass() {
		return DistinctTypeData.ModeData.class;
	}
}