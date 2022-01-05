package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.LastDistinctValueUdaf;
import com.alibaba.alink.common.sql.builtin.agg.LastDistinctValueUdaf.LastDistinctSaveFirst;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LastDistinctTest extends AggFunctionTestBase <Object[], Object, LastDistinctSaveFirst> {

	@Before
	public void init() {
		useRetract = false;
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();

		List <Object[]> data = Arrays.asList(
			new Object[] {1, 0, 1L, 0.1},
			new Object[] {1, 2, 2L, 0.1},
			new Object[] {3, 1, 3L, 0.1},
			new Object[] {3, 3, 4L, 0.1},
			new Object[] {4, 3, 5L, 0.1},
			new Object[] {4, 0, 6L, 0.1},
			new Object[] {2, 0, 7L, 0.1},
			new Object[] {2, 3, 8L, 0.1});

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
		return Arrays.asList(null, null, 2, 2, 3, 3, 0, 0);
	}

	@Override
	protected AggregateFunction <Object, LastDistinctSaveFirst> getAggregator() {
		return new LastDistinctValueUdaf(0.5);
	}

	@Override
	protected Class <?> getAccClass() {
		return LastDistinctSaveFirst.class;
	}
}
