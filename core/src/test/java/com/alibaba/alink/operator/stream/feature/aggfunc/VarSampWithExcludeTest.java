package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.BaseSummaryUdaf.SummaryData;
import com.alibaba.alink.common.sql.builtin.agg.VarSampUdaf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class VarSampWithExcludeTest extends AggFunctionTestBase <Object[], Object, SummaryData> {

	@Before
	public void init() {
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();
		ArrayList <Object[]> data1 = new ArrayList <>();
		data1.add(new Object[] {1.0});
		res.add((List <Object[]>) data1.clone());
		data1.add(new Object[] {2.0});
		res.add((List <Object[]>) data1.clone());
		data1.add(new Object[] {3.0});
		res.add((List <Object[]>) data1.clone());
		data1.add(new Object[] {4.0});
		res.add((List <Object[]>) data1.clone());
		data1.add(new Object[] {5.0});
		res.add((List <Object[]>) data1.clone());
		data1.add(new Object[] {6.0});
		res.add((List <Object[]>) data1.clone());
		data1.add(new Object[] {7.0});
		res.add((List <Object[]>) data1.clone());
		data1.add(new Object[] {8.0});
		res.add(data1);
		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		List <Object> res = new ArrayList <>();
		res.add(0.0);
		res.add(0.0);
		res.add(0.5);
		res.add(1.0);
		res.add(1.6667);
		res.add(2.5);
		res.add(3.5);
		res.add(4.6667);
		return res;
	}

	@Override
	protected AggregateFunction <Object, SummaryData> getAggregator() {
		return new VarSampUdaf(true);
	}

	@Override
	protected Class <?> getAccClass() {
		return SummaryData.class;
	}
}
