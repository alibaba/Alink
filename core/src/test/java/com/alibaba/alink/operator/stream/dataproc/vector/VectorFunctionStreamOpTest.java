package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class VectorFunctionStreamOpTest extends AlinkTestBase {
	@Test
	public void testVectorFunctionSparse() throws Exception {
		List <Row> df = new ArrayList <>();
		df.add(Row.of("$8$1:3,2:4,4:7", "$8$1:3,2:4,4:7"));
		StreamOperator <?> data = new MemSourceStreamOp(df, "vec1 string, vec2 string");
		VectorFunctionStreamOp vecFunc = new VectorFunctionStreamOp()
			.setSelectedCol("vec1")
			.setFuncName("normL2")
			.setReservedCols(new String[] {})
			.setOutputCol("vec_normL2");

		StreamOperator <?> res = vecFunc.linkFrom(data);
		CollectSinkStreamOp sop = res.link(new CollectSinkStreamOp());
		StreamOperator.execute();
		List <Row> list = sop.getAndRemoveValues();
		for (Row row : list) {
			Assert.assertEquals((double) row.getField(0), 8.6023, 0.001);
		}
	}

	@Test
	public void testVectorFunctionDense() throws Exception {
		List <Row> df = new ArrayList <>();
		df.add(Row.of("3 4 7", "2 3 4"));
		StreamOperator <?> data = new MemSourceStreamOp(df, "vec1 string, vec2 string");

		VectorFunctionStreamOp vecFunc = new VectorFunctionStreamOp()
			.setSelectedCol("vec1")
			.setFuncName("normL2Square")
			.setReservedCols(new String[] {})
			.setOutputCol("vec_normL2");

		StreamOperator <?> res = vecFunc.linkFrom(data);
		CollectSinkStreamOp sop = res.link(new CollectSinkStreamOp());
		StreamOperator.execute();
		List <Row> list = sop.getAndRemoveValues();
		for (Row row : list) {
			Assert.assertEquals((double) row.getField(0), 74.0, 0.001);
		}
	}
}