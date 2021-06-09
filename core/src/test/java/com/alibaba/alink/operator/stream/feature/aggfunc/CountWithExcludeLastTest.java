package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.BaseSummaryUdaf.SummaryData;
import com.alibaba.alink.common.sql.builtin.agg.CountUdaf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class CountWithExcludeLastTest extends AggFunctionTestBase <Object[], Object, SummaryData> {

	@Before
	public void init() {
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();
		ArrayList <Object[]> data1 = new ArrayList <>();
		res.add((List <Object[]>) data1.clone());
		data1.add(new Object[] {1});
		res.add((List <Object[]>) data1.clone());
		data1.add(new Object[] {4});
		res.add(data1);
		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		List <Object> res = new ArrayList <>();
		res.add(0L);
		res.add(0L);
		res.add(1L);
		return res;
	}

	@Override
	protected AggregateFunction <Object, SummaryData> getAggregator() {
		return new CountUdaf(true);
	}

	@Override
	protected Class <?> getAccClass() {
		return SummaryData.class;
	}
}
