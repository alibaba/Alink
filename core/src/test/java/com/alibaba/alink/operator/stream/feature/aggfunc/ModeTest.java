package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.DistinctTypeData.ModeData;
import com.alibaba.alink.common.sql.builtin.agg.ModeUdaf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class ModeTest extends AggFunctionTestBase <Object[], Object, ModeData> {

	@Before
	public void init() {
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();
		ArrayList <Object[]> data = new ArrayList <>();
		data.add(new Object[] {2});
		data.add(new Object[] {2});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {8});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {8});
		data.add(new Object[] {8});
		res.add((List <Object[]>) data.clone());
		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		List <Object> res = new ArrayList <>();
		res.add(2);
		res.add(2);
		res.add(8);
		return res;
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
