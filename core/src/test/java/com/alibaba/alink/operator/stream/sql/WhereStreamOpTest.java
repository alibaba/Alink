package com.alibaba.alink.operator.stream.sql;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class WhereStreamOpTest extends AlinkTestBase {

	@Test
	public void testWhereStreamOp() throws Exception {
		List <Row> inputRows = Arrays.asList(
			Row.of("a", 1, 1.1, 1.2),
			Row.of("b", -2, 0.9, 1.0),
			Row.of("c", 100, -0.01, 1.0),
			Row.of("d", -99, 100.9, 0.1),
			Row.of("a", 1, 1.1, 1.2),
			Row.of("b", -2, 0.9, 1.0),
			Row.of("c", 100, -0.01, 0.2),
			Row.of("d", -99, 100.9, 0.3)
		);
		StreamOperator <?> source = new MemSourceStreamOp(inputRows,
			"col1 string, col2 int, col3 double, col4 double");
		StreamOperator <?> output = source.link(new WhereStreamOp().setClause("col1='a'"));
		CollectSinkStreamOp sink = output.link(new CollectSinkStreamOp());
		List <Row> expectedResult = Arrays.asList(
			Row.of("a", 1, 1.1, 1.2),
			Row.of("a", 1, 1.1, 1.2));
		StreamOperator.execute();
		List <Row> result = sink.getAndRemoveValues();
		compareResultCollections(expectedResult, result, (o1, o2) -> 0);
	}
}