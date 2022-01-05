package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.VectorUtil;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class VectorBiFunctionStreamOpTest extends AlinkTestBase {
	@Test
	public void testBiVectorFunctionSparse() throws Exception {
		List <Row> df = new ArrayList <>();
		df.add(Row.of("$8$1:3,2:4,4:7", "$8$1:3,2:4,4:7"));
		StreamOperator <?> data = new MemSourceStreamOp(df, "vec1 string, vec2 string");
		VectorBiFunctionStreamOp vecBiFunc = new VectorBiFunctionStreamOp()
			.setSelectedCols("vec1", "vec2")
			.setBiFuncName("minus")
			.setReservedCols(new String[] {})
			.setOutputCol("vec_minus");
		StreamOperator <?> res = vecBiFunc.linkFrom(data);
		CollectSinkStreamOp sop = res.link(new CollectSinkStreamOp());
		StreamOperator.execute();
		List <Row> list = sop.getAndRemoveValues();
		for (Row row : list) {
			Assert.assertEquals(row.getField(0), VectorUtil.parseSparse("$8$1:0.0 2:0.0 4:0.0"));
		}
	}

	@Test
	public void testBiVectorFunctionDense() throws Exception {
		List <Row> df = new ArrayList <>();
		df.add(Row.of("1 2 3", "2 3 4"));
		StreamOperator <?> data = new MemSourceStreamOp(df, "vec1 string, vec2 string");

		VectorBiFunctionStreamOp vecBiFunc = new VectorBiFunctionStreamOp()
			.setSelectedCols("vec1", "vec2")
			.setBiFuncName("minus")
			.setReservedCols(new String[] {})
			.setOutputCol("vec_minus");
		StreamOperator <?> res = vecBiFunc.linkFrom(data);
		CollectSinkStreamOp sop = res.link(new CollectSinkStreamOp());
		StreamOperator.execute();
		List <Row> list = sop.getAndRemoveValues();
		for (Row row : list) {
			Assert.assertEquals(row.getField(0), VectorUtil.parseDense("-1.0 -1.0 -1.0"));
		}
	}
}
