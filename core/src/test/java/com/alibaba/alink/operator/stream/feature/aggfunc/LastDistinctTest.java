package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.LastDistinctValueUdaf;
import com.alibaba.alink.common.sql.builtin.agg.LastDistinctValueUdaf.LastDistinctSaveFirst;
import org.junit.Before;

import java.util.ArrayList;
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
		ArrayList <Object[]> data = new ArrayList <>();
		data.add(new Object[] {1, 0, 1L, 0.1});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {1, 2, 2L, 0.1});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {3, 1, 3L, 0.1});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {3, 3, 4L, 0.1});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {4, 3, 5L, 0.1});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {4, 0, 6L, 0.1});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {2, 0, 7L, 0.1});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {2, 3, 8L, 0.1});
		res.add((List <Object[]>) data.clone());
		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		List <Object> res = new ArrayList <>();
		res.add(null);
		res.add(null);
		res.add(2);
		res.add(2);
		res.add(3);
		res.add(3);
		res.add(0);
		res.add(0);
		return res;
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
