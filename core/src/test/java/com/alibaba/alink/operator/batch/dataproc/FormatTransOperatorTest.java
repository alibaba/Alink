package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.format.*;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.types.Row;
import org.junit.Test;

public class FormatTransOperatorTest {
	private static Row[] rows = new Row[] {
		Row.of(1L, "a,2.0,3.0", "f1=a,f2=2.0,f3=3.0", "{\"f1\":\"a\",\"f2\":2.0,\"f3\":3.0}", "2.0,3.0"),
		Row.of(2L, "a,2.0,3.b", "f1=a,f2=2.0,f3=3.0", "{\"f1\":\"a\",\"f2\":2.0,\"f3\":\"3.b\"}", "2.0,3.b"),
	};

	private static BatchOperator data = new MemSourceBatchOp(rows,
		new String[] {"id", "csv", "kv", "json", "vec"});

	@Test
	public void testToColumns() throws Exception {
		BatchOperator output1 = new CsvToColumnsBatchOp().setCsvCol("csv").setReservedCols("id")
			.setSchemaStr("f1 string, f2 double, f3 string").linkFrom(data).lazyCollect();
		BatchOperator output2 = new KvToColumnsBatchOp().setKvCol("kv").setReservedCols("id")
			.setKvValDelimiter("=")
			.setSchemaStr("f1 string, f2 double, f3 double").linkFrom(data).lazyCollect();
		BatchOperator output3 = new JsonToColumnsBatchOp().setJsonCol("json").setReservedCols("id")
			.setSchemaStr("f1 string, f2 double, f3 string").linkFrom(data).lazyCollect();
		BatchOperator.execute();
	}

	@Test
	public void testFromColumns() throws Exception {
		BatchOperator output1 = new ColumnsToCsvBatchOp().setSelectedCols("id", "csv").setReservedCols("id")
			.setSchemaStr("id bigint, csv string")
			.setCsvCol("csv").linkFrom(data).lazyCollect();
		BatchOperator output2 = new ColumnsToKvBatchOp().setKvCol("kv").setSelectedCols("id", "kv").setReservedCols(
			"id")
			.setKvColDelimiter("~")
			.linkFrom(data).lazyCollect();
		BatchOperator output3 = new ColumnsToJsonBatchOp().setJsonCol("json").setReservedCols("id")
			.setSelectedCols("id", "json")
			.linkFrom(data).lazyCollect();
		BatchOperator.execute();
	}

}