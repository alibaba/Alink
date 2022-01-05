package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.BaseSummaryUdaf.SummaryData;
import com.alibaba.alink.common.sql.builtin.agg.StddevSampUdaf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StddevSampWithExcludeTest extends AggFunctionTestBase <Object[], Object, SummaryData> {

	@Before
	public void init() {
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();

		List <Object[]> data = Arrays.asList(
			new Object[] {1.0},
			new Object[] {2.0},
			new Object[] {3.0},
			new Object[] {4.0},
			new Object[] {5.0},
			new Object[] {6.0},
			new Object[] {7.0},
			new Object[] {8.0}
		);

		res.add(data.subList(0, 1));
		res.add(data.subList(0, 2));
		res.add(data.subList(0, 3));
		res.add(data.subList(0, 4));
		res.add(data.subList(0, 5));
		res.add(data.subList(0, 6));
		res.add(data.subList(0, 7));
		res.add(data.subList(0, 8));

		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		return Arrays.asList(0.0, 0.0, 0.7071, 1.0, 1.2910, 1.5811, 1.8708, 2.1602);
	}

	@Override
	protected AggregateFunction <Object, SummaryData> getAggregator() {
		return new StddevSampUdaf(true);
	}

	@Override
	protected Class <?> getAccClass() {
		return SummaryData.class;
	}
}
