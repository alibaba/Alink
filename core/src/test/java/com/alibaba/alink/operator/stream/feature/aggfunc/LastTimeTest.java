package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.LastTimeUdaf;
import com.alibaba.alink.common.sql.builtin.agg.LastValueTypeData.LastTimeData;
import org.junit.Before;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class LastTimeTest extends AggFunctionTestBase <Object[], Object, LastTimeData> {

	@Before
	public void init() {
		useRetract = false;
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();
		ArrayList <Object[]> data = new ArrayList <>();
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {0, new Timestamp(1L), 120});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {0, new Timestamp(2L), 120});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {0, new Timestamp(3L), 120});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {0, new Timestamp(4L), 120});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {0, new Timestamp(5L), 120});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {0, new Timestamp(6L), 120});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {0, new Timestamp(7L), 120});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {0, new Timestamp(8L), 120});
		res.add((List <Object[]>) data.clone());
		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		List <Object> res = new ArrayList <>();
		res.add(null);
		res.add(new Timestamp(1L));
		res.add(new Timestamp(2L));
		res.add(new Timestamp(3L));
		res.add(new Timestamp(4L));
		res.add(new Timestamp(5L));
		res.add(new Timestamp(6L));
		res.add(new Timestamp(7L));
		res.add(new Timestamp(8L));
		return res;
	}

	@Override
	protected AggregateFunction <Object, LastTimeData> getAggregator() {
		return new LastTimeUdaf(2, 0.5);
	}

	@Override
	protected Class <?> getAccClass() {
		return LastTimeData.class;
	}
}
