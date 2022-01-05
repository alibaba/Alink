package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.LastTimeUdaf;
import com.alibaba.alink.common.sql.builtin.agg.LastValueTypeData.LastTimeData;
import org.junit.Before;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LastTimeTest extends AggFunctionTestBase <Object[], Object, LastTimeData> {

	@Before
	public void init() {
		useRetract = false;
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();

		List <Object[]> data = Arrays.asList(
			new Object[] {0, new Timestamp(1L), 120},
			new Object[] {0, new Timestamp(2L), 120},
			new Object[] {0, new Timestamp(3L), 120},
			new Object[] {0, new Timestamp(4L), 120},
			new Object[] {0, new Timestamp(5L), 120},
			new Object[] {0, new Timestamp(6L), 120},
			new Object[] {0, new Timestamp(7L), 120},
			new Object[] {0, new Timestamp(8L), 120});

		res.add(new ArrayList <>());
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
		return Arrays.asList(null,
			new Timestamp(1L),
			new Timestamp(2L),
			new Timestamp(3L),
			new Timestamp(4L),
			new Timestamp(5L),
			new Timestamp(6L),
			new Timestamp(7L),
			new Timestamp(8L));
	}

	@Override
	protected AggregateFunction <Object, LastTimeData> getAggregator() {
		return new LastTimeUdaf(2, 0.5);
	}

	@Override
	protected Class <?> getAccClass() {
		return LastTimeData.class;
	}
}
