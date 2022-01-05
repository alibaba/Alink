package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.MedianUdaf;
import com.alibaba.alink.common.sql.builtin.agg.MedianUdaf.MedianData;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MedianWithExcludeTest extends AggFunctionTestBase <Object[], Number, MedianData> {

	@Before
	public void init() {
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();

		List <Object[]> data = Arrays.asList(
			new Object[] {2},
			new Object[] {1},
			new Object[] {3},
			new Object[] {8},
			new Object[] {5}
		);

		res.add(data.subList(0, 1));
		res.add(data.subList(0, 2));
		res.add(data.subList(0, 3));
		res.add(data.subList(0, 5));

		return res;
	}

	@Override
	protected List <Number> getExpectedResults() {
		return Arrays.asList(0, 2, 1, 2);
	}

	@Override
	protected AggregateFunction <Number, MedianData> getAggregator() {
		return new MedianUdaf(true);
	}

	@Override
	protected Class <?> getAccClass() {
		return MedianData.class;
	}
}
