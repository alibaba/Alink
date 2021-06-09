package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.MinUdaf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class MinTest extends AggFunctionTestBase <Object[], Object, MinUdaf.MinMaxData> {

	@Before
	public void init() {
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();
		ArrayList <Object[]> data = new ArrayList <>();
		data.add(new Object[] {9});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {7});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {8});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {6});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {7});
		res.add((List <Object[]>) data.clone());
		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		List <Object> res = new ArrayList <>();
		res.add(9);
		res.add(7);
		res.add(7);
		res.add(6);
		res.add(6);
		return res;
	}

	@Override
	protected AggregateFunction <Object, MinUdaf.MinMaxData> getAggregator() {
		return new MinUdaf();
	}

	@Override
	protected Class <?> getAccClass() {
		return MinUdaf.MinMaxData.class;
	}
}