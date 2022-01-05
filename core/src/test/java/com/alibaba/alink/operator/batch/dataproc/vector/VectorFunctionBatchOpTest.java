package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class VectorFunctionBatchOpTest extends AlinkTestBase {
	@Test
	public void testVectorFunctionSparse() {
		List <Row> df = new ArrayList <>();
		df.add(Row.of("$8$1:3,2:4,4:7", "$8$1:3,2:4,4:7"));
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec1 string, vec2 string");
		VectorFunctionBatchOp vecFunc = new VectorFunctionBatchOp()
			.setSelectedCol("vec1")
			.setFuncName("normL2")
			.setReservedCols(new String[] {})
			.setOutputCol("vec_normL2");
		List <Row> res = vecFunc.linkFrom(data).collect();
		Assert.assertEquals((double) res.get(0).getField(0), 8.6023, 0.001);
	}

	@Test
	public void testVectorFunctionDense() {
		List <Row> df = new ArrayList <>();
		df.add(Row.of("3 4 7", "2 3 4"));
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec1 string, vec2 string");

		VectorFunctionBatchOp vecFunc = new VectorFunctionBatchOp()
			.setSelectedCol("vec1")
			.setFuncName("normL2Square")
			.setReservedCols(new String[] {})
			.setOutputCol("vec_normL2");
		List <Row> res = vecFunc.linkFrom(data).collect();
		Assert.assertEquals((double) res.get(0).getField(0), 74.0, 0.001);
	}
}
