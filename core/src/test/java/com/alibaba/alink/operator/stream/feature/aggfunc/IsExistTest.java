package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.DistinctTypeData.IsExistData;
import com.alibaba.alink.common.sql.builtin.agg.IsExistUdaf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class IsExistTest extends AggFunctionTestBase <Object[], Boolean, IsExistData> {

	@Before
	public void init() {
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();
		ArrayList <Object[]> data = new ArrayList <>();
		data.add(new Object[] {1});
		data.add(new Object[] {1});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {2});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {2});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {3});
		res.add((List <Object[]>) data.clone());
		return res;
	}

	@Override
	protected List <Boolean> getExpectedResults() {
		List <Boolean> res = new ArrayList <>();
		res.add(true);
		res.add(false);
		res.add(true);
		res.add(false);
		return res;
	}

	@Override
	protected AggregateFunction <Boolean, IsExistData> getAggregator() {
		return new IsExistUdaf();
	}

	@Override
	protected Class <?> getAccClass() {
		return IsExistData.class;
	}
}
