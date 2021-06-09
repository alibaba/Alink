package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.LastValueTypeData.SumLastData;
import com.alibaba.alink.common.sql.builtin.agg.SumLastUdaf;
import org.junit.Before;

import java.util.ArrayList;
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
		ArrayList <Object[]> data = new ArrayList <>();
		data.add(new Object[] {1, 1L, 0.1});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {1, 2L, 0.1});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {3, 3L, 0.1});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {3, 4L, 0.1});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {4, 5L, 0.1});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {4, 6L, 0.1});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {2, 7L, 0.1});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {2, 8L, 0.1});
		res.add((List <Object[]>) data.clone());
		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		List <Object> res = new ArrayList <>();
		res.add(1);
		res.add(1);
		res.add(3);
		res.add(3);
		res.add(4);
		res.add(4);
		res.add(2);
		res.add(2);
		return res;
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
