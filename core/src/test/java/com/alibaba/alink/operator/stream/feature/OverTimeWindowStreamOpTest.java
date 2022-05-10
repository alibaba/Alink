package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class OverTimeWindowStreamOpTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		List <Row> sourceFrame = Arrays.asList(
			Row.of("Mary", Timestamp.valueOf("2021-11-11 12:00:00"), 10),
			Row.of("Bob", Timestamp.valueOf("2021-11-11 12:20:00"), 20),
			Row.of("Mary", Timestamp.valueOf("2021-11-11 12:30:11"), 30),
			Row.of("Mary", Timestamp.valueOf("2021-11-11 12:35:15"), 40),
			Row.of("Bob", Timestamp.valueOf("2021-11-11 13:10:00"), 50),
			Row.of("Liz", Timestamp.valueOf("2021-11-11 13:29:00"), 60),
			Row.of("Liz", Timestamp.valueOf("2021-11-11 13:40:23"), 70),
			Row.of("Mary", Timestamp.valueOf("2021-11-11 14:02:00"), 80),
			Row.of("Liz", Timestamp.valueOf("2021-11-11 14:35:10"), 90),
			Row.of("Bob", Timestamp.valueOf("2021-11-11 15:02:20"), 100),
			Row.of("Bob", Timestamp.valueOf("2021-11-11 15:02:25"), 110)
		);

		StreamOperator <?> streamSource = new MemSourceStreamOp(
			sourceFrame,
			new String[] {"user", "time1", "money"});

		StreamOperator <?> op = new OverTimeWindowStreamOp()
			.setGroupCols("user")
			.setTimeCol("time1")
			.setPrecedingTime("1h")
			.setClause(
				"listagg_preceding(money) as list_money");

		streamSource.link(op).print();

		StreamOperator.execute();

	}

	@Test
	public void test2() throws Exception {
		List <Row> sourceFrame = Arrays.asList(
			Row.of("Mary", Timestamp.valueOf("2021-11-11 12:00:00"), 10),
			Row.of("Bob", Timestamp.valueOf("2021-11-11 12:20:00"), 20),
			Row.of("Mary", Timestamp.valueOf("2021-11-11 12:30:11"), 30),
			Row.of("Mary", Timestamp.valueOf("2021-11-11 12:35:15"), 40),
			Row.of("Bob", Timestamp.valueOf("2021-11-11 13:10:00"), 50),
			Row.of("Liz", Timestamp.valueOf("2021-11-11 13:29:00"), 60),
			Row.of("Liz", Timestamp.valueOf("2021-11-11 13:40:23"), 70),
			Row.of("Mary", Timestamp.valueOf("2021-11-11 14:02:00"), 80),
			Row.of("Liz", Timestamp.valueOf("2021-11-11 14:35:10"), 90),
			Row.of("Bob", Timestamp.valueOf("2021-11-11 15:02:20"), 100),
			Row.of("Bob", Timestamp.valueOf("2021-11-11 15:02:25"), 110)
		);

		StreamOperator <?> streamSource = new MemSourceStreamOp(
			sourceFrame,
			new String[] {"user", "time1", "money"});

		StreamOperator <?> op = new OverTimeWindowStreamOp()
			.setGroupCols("user")
			.setTimeCol("time1")
			.setPrecedingTime(3600)
			.setClause(
				"listagg_preceding(money) as list_money");

		streamSource.link(op).print();

		StreamOperator.execute();

	}

}