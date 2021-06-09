package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.functions.AggregateFunction;

import com.alibaba.alink.common.sql.builtin.agg.LastValueTypeData.LastValueData;
import com.alibaba.alink.common.sql.builtin.agg.LastValueUdaf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class LastValueTest extends AggFunctionTestBase <Object[], Object, LastValueData> {
	@Before
	public void init() {
		useRetract = false;
		multiInput = true;
	}

	@Override
	protected List <List <Object[]>> getInputValueSets() {
		List <List <Object[]>> res = new ArrayList <>();
		ArrayList <Object[]> data = new ArrayList <>();
		data.add(new Object[] {0, 1L, 120});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {0, 2L, 120});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {0, 3L, 120});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {0, 4L, 120});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {0, 5L, 120});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {0, 6L, 120});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {0, 7L, 120});
		res.add((List <Object[]>) data.clone());
		data.add(new Object[] {0, 8L, 120});
		res.add((List <Object[]>) data.clone());
		return res;
	}

	@Override
	protected List <Object> getExpectedResults() {
		List <Object> res = new ArrayList <>();
		res.add(0L);
		res.add(0L);
		res.add(0L);
		res.add(0L);
		res.add(0L);
		res.add(0L);
		res.add(0L);
		res.add(0L);
		return res;
	}

	@Override
	protected AggregateFunction <Object, LastValueData> getAggregator() {
		return new LastValueUdaf();
	}

	@Override
	protected Class <?> getAccClass() {
		return LastValueData.class;
	}
}
