package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorBiFunctionBatchOpTest extends AlinkTestBase {
	@Test
	public void testBiVectorFunctionSparse() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("$8$1:3,2:4,4:7", "$8$1:3,2:4,4:7")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec1 string, vec2 string");
		VectorBiFunctionBatchOp vecBiFunc = new VectorBiFunctionBatchOp()
			.setSelectedCols("vec1", "vec2")
			.setBiFuncName("minus")
			.setReservedCols(new String[]{})
			.setOutputCol("vec_minus");
		List<Row> res = vecBiFunc.linkFrom(data).collect();
		Assert.assertEquals(res.get(0), Row.of(VectorUtil.parseSparse("$8$1:0.0 2:0.0 4:0.0")));
	}

	@Test
	public void testBiVectorFunctionDense() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1 2 3", "2 3 4")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec1 string, vec2 string");

		VectorBiFunctionBatchOp vecBiFunc = new VectorBiFunctionBatchOp()
			.setSelectedCols("vec1", "vec2")
			.setBiFuncName("minus")
			.setReservedCols(new String[]{})
			.setOutputCol("vec_minus");
		List<Row> res = vecBiFunc.linkFrom(data).collect();
		Assert.assertEquals(res.get(0), Row.of(VectorUtil.parseDense("-1.0 -1.0 -1.0")));
	}
}
