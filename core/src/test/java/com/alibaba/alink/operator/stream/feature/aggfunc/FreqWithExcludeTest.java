package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.DistinctTypeData.FreqData;
import com.alibaba.alink.common.sql.builtin.agg.FreqUdaf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class FreqWithExcludeTest extends AggFunctionTestBase <Object[], Long, FreqData> {

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
	protected List <Long> getExpectedResults() {
		List <Long> res = new ArrayList <>();
		res.add(1L);
		res.add(0L);
		res.add(1L);
		res.add(0L);
		return res;
	}

	@Override
	protected AggregateFunction <Long, FreqData> getAggregator() {
		return new FreqUdaf(true);
	}

	@Override
	protected Class <?> getAccClass() {
		return FreqData.class;
	}
}