package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

public class TypeConvertBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"1", 1, 1.1, 1.0, true}),
				Row.of(new Object[] {"2", -2, 0.9, 2.0, false}),
				Row.of(new Object[] {"3", 100, -0.01, 3.0, true}),
				Row.of(new Object[] {"4", -99, null, 4.0, false}),
				Row.of(new Object[] {"5", 1, 1.1, 5.0, true}),
				Row.of(new Object[] {"6", -2, 0.9, 6.0, false})
			};
		String[] colnames = new String[] {"group", "col2", "col3", "col4", "col5"};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);

		Params p = new Params().set("newType", "BIGINT").set("selectedColNames", new String[] {"group"});
		TypeConvertBatchOp typeConvertBatchOp = new TypeConvertBatchOp(p);

		int s = inOp.link(typeConvertBatchOp).collect().size();
		Assert.assertEquals(s, 6);
	}

	@Test
	public void test2() throws Exception {
		String schemaStr = "f0 string, f1 int, f2 float, f3 DECIMAL";

		List <Row> rows = Arrays.asList(
			Row.of("a", 1, 0.2f, new BigDecimal("1e10")),
			Row.of("a", 2, 0.3f, new BigDecimal("1.2e-5"))
		);

		BatchOperator <?> mSource = new MemSourceBatchOp(rows, schemaStr);

		BatchOperator<?> typeConvert = new TypeConvertBatchOp()
			.setTargetType("double")
			.setSelectedCols("f3", "f2");

		mSource.link(typeConvert);

		typeConvert.getOutputTable().printSchema();

		typeConvert.lazyPrint();

		BatchOperator <?> mSource2 = new MemSourceBatchOp(rows, schemaStr);

		BatchOperator<?> typeConvert2 = new TypeConvertBatchOp()
			.setTargetType("decimal")
			.setSelectedCols("f3", "f2");

		mSource2.link(typeConvert2);

		typeConvert2.getOutputTable().printSchema();

		typeConvert2.lazyPrint();

		BatchOperator.execute();
	}
}
