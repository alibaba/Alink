package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.DistinctTypeData;
import com.alibaba.alink.common.sql.builtin.agg.ModeUdaf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class ModeWithExcludeTest extends AggFunctionTestBase <Object[], Object, DistinctTypeData.ModeData> {

	@Before
	public void init() {
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();
		ArrayList <Object[]> data = new ArrayList <>();
		data.add(new Object[] {2});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {2});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {8});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {8});
		data.add(new Object[] {8});
		data.add(new Object[] {8});
		res.add((List <Object[]>) data.clone());
		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		List <Object> res = new ArrayList <>();
		res.add(null);
		res.add(2);
		res.add(2);
		res.add(8);
		return res;
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