package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class KvToCsvStreamOpTest extends AlinkTestBase {
	@Test
	public void testKvToCsvStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1", "{\"f0\":\"1.0\",\"f1\":\"2.0\"}", "$3$0:1.0 1:2.0", "f0:1.0,f1:2.0", "1.0,2.0", 1.0, 2.0)
		);
		StreamOperator <?> data = new MemSourceStreamOp(df,
			"row string, json string, vec string, kv string, csv string, f0 double, f1 double");
		StreamOperator <?> op = new KvToCsvStreamOp()
			.setKvCol("kv")
			.setReservedCols("row")
			.setCsvCol("csv")
			.setSchemaStr("f0 double, f1 double")
			.linkFrom(data);
		op.print();
		StreamOperator.execute();
	}
}