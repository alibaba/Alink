package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.ListAggUdaf;
import com.alibaba.alink.common.sql.builtin.agg.ListAggUdaf.ListAggData;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class ListAggExcludeLastTest extends AggFunctionTestBase <Object[], String, ListAggData> {

	@Before
	public void init() {
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <Object[]> data1 = new ArrayList <>();
		data1.add(new Object[] {"q"});
		data1.add(new Object[] {"w"});
		data1.add(new Object[] {"e"});
		List <Object[]> data2 = new ArrayList <>();
		data2.add(new Object[] {"a"});
		data2.add(new Object[] {"s"});
		List <Object[]> data3 = new ArrayList <>();
		data3.add(new Object[] {"a"});
		List <List <Object[]>> res = new ArrayList <>();
		res.add(data1);
		res.add(data2);
		res.add(data3);
		return res;
	}

	@Override
	protected List <String> getExpectedResults() {
		List <String> res = new ArrayList <>();
		res.add("q,w");
		res.add("a");
		res.add(null);
		return res;
	}

	@Override
	protected AggregateFunction <String, ListAggData> getAggregator() {
		return new ListAggUdaf(true);
	}

	@Override
	protected Class <?> getAccClass() {
		return ListAggData.class;
	}
}
