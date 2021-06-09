package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.MaxUdaf;
import com.alibaba.alink.common.sql.builtin.agg.MinUdaf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class MaxTest extends AggFunctionTestBase <Object[], Object, MinUdaf.MinMaxData> {

	@Before
	public void init() {
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();
		ArrayList <Object[]> data = new ArrayList <>();
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {1});
		data.add(new Object[] {3});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {2});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {4});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {3});
		res.add((List <Object[]>) data.clone());
		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		List <Object> res = new ArrayList <>();
		res.add(null);
		res.add(3);
		res.add(3);
		res.add(4);
		res.add(4);
		return res;
	}

	@Override
	protected AggregateFunction <Object, MinUdaf.MinMaxData> getAggregator() {
		return new MaxUdaf();
	}

	@Override
	protected Class <?> getAccClass() {
		return MinUdaf.MinMaxData.class;
	}
}
