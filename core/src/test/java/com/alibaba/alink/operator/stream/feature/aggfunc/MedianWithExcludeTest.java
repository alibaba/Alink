package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.MedianUdaf;
import com.alibaba.alink.common.sql.builtin.agg.MedianUdaf.MedianData;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class MedianWithExcludeTest extends AggFunctionTestBase <Object[], Number, MedianData> {

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
		data.add(new Object[] {1});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {3});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {8});
		data.add(new Object[] {5});
		res.add((List <Object[]>) data.clone());
		return res;
	}

	@Override
	protected List <Number> getExpectedResults() {
		List <Number> res = new ArrayList <>();
		res.add(0);
		res.add(2);
		res.add(1);
		res.add(2);
		return res;
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
