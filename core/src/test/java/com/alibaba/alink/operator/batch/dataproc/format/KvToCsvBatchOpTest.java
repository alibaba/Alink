package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class KvToCsvBatchOpTest extends AlinkTestBase {
	@Test
	public void testKvToCsvBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1", "{\"f0\":\"1.0\",\"f1\":\"2.0\"}", "$3$0:1.0 1:2.0", "f0:1.0,f1:2.0", "1.0,2.0", 1.0, 2.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df,
			"row string, json string, vec string, kv string, csv string, f0 double, f1 double");
		BatchOperator <?> op = new KvToCsvBatchOp()
			.setKvCol("kv")
			.setReservedCols("row")
			.setCsvCol("csv")
			.setSchemaStr("f0 double, f1 double")
			.linkFrom(data);
		op.print();
	}
}