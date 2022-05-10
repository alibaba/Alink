package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class OverCountWindowStreamOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		List <Row> sourceFrame = Arrays.asList(
			Row.of("Mary", Timestamp.valueOf("2021-11-11 12:00:00"), 10.0),
			Row.of("Bob", Timestamp.valueOf("2021-11-11 12:00:00"), 20.0),
			Row.of("Mary", Timestamp.valueOf("2021-11-11 12:00:11"), 30.0),
			Row.of("Mary", Timestamp.valueOf("2021-11-11 12:00:15"), 40.0),
			Row.of("Bob", Timestamp.valueOf("2021-11-11 12:01:00"), 50.0),
			Row.of("Liz", Timestamp.valueOf("2021-11-11 12:01:00"), 60.0),
			Row.of("Liz", Timestamp.valueOf("2021-11-11 12:01:23"), 70.0),
			Row.of("Mary", Timestamp.valueOf("2021-11-11 12:02:00"), 80.0),
			Row.of("Liz", Timestamp.valueOf("2021-11-11 12:02:10"), 90.0),
			Row.of("Bob", Timestamp.valueOf("2021-11-11 12:02:20"), 100.0),
			Row.of("Bob", Timestamp.valueOf("2021-11-11 12:02:25"), 110.0)
		);

		StreamOperator <?> streamSource = new MemSourceStreamOp(
			sourceFrame,
			new String[] {"user", "time1", "money"});

		StreamOperator <?> op = new OverCountWindowStreamOp()
			.setGroupCols("user")
			.setTimeCol("time1")
			.setPrecedingRows(3)
			.setClause("avg_preceding(money) as avg_money")
			.setReservedCols("user", "time1", "money");

		streamSource.link(op).print();

		StreamOperator.execute();
	}

}