package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.LagUdaf;

import com.alibaba.alink.common.sql.builtin.agg.LastValueTypeData.LagData;
import com.alibaba.alink.common.sql.builtin.agg.LastValueTypeData.LastValueData;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class LagWithDefaultTest extends AggFunctionTestBase <Object[], Object, LagData> {

	@Before
	public void init() {
		useRetract = false;
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();
		ArrayList <Object[]> data = new ArrayList <>();
		data.add(new Object[] {1, 2, 33});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {1, 2, 33});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {3, 2, 33});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {3, 2, 33});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {4, 2, 33});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {4, 2, 33});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {2, 2, 33});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {2, 2, 33});
		res.add((List <Object[]>) data.clone());
		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		List <Object> res = new ArrayList <>();
		res.add(33);
		res.add(33);
		res.add(1);
		res.add(1);
		res.add(3);
		res.add(3);
		res.add(4);
		res.add(4);
		return res;
	}

	@Override
	protected AggregateFunction <Object, LagData> getAggregator() {
		return new LagUdaf();
	}

	@Override
	protected Class <?> getAccClass() {
		return LagData.class;
	}
}
