package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.shared.HasBiFuncName.BiFuncName;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class VectorBiFunctionTest extends AlinkTestBase {
	@Test
	public void testBiVectorFunctionSparse() {
		List <Row> df = new ArrayList <>();
		df.add(Row.of("$8$1:3,2:4,4:7", "$8$1:3,2:4,4:7"));
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec1 string, vec2 string");
		VectorBiFunction vecBiFunc = new VectorBiFunction()
			.setSelectedCols("vec1", "vec2")
			.setBiFuncName("minus")
			.setReservedCols("vec1", "vec2")
			.setOutputCol("vec_minus");
		BatchOperator <?> res = vecBiFunc.transform(data);

		vecBiFunc.setBiFuncName("plus").setOutputCol("vec_plus").setReservedCols("vec_minus", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.ElementWiseMultiply).setOutputCol("vec_multiply")
			.setReservedCols("vec_minus", "vec_plus", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.Dot).setOutputCol("vec_dot")
			.setReservedCols("vec_minus", "vec_plus", "vec_multiply", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.EuclidDistance).setOutputCol("vec_distance")
			.setReservedCols("vec_minus", "vec_plus", "vec_multiply", "vec_dot", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.Cosine).setOutputCol("vec_cos")
			.setReservedCols("vec_minus", "vec_plus", "vec_multiply", "vec_dot", "vec_distance", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.Merge).setOutputCol("vec_merge")
			.setReservedCols("vec_minus", "vec_plus", "vec_multiply", "vec_dot", "vec_distance", "vec_cos");
		List <Row> result = vecBiFunc.transform(res).collect();

		Assert.assertEquals(result.get(0).getField(0), VectorUtil.parseSparse("$8$1:0.0 2:0.0 4:0.0"));
		Assert.assertEquals(result.get(0).getField(1), VectorUtil.parseSparse("$8$1:6.0 2:8.0 4:14.0"));
		Assert.assertEquals(result.get(0).getField(2), VectorUtil.parseSparse("$8$1:9.0 2:16.0 4:49.0"));
		Assert.assertEquals((double) result.get(0).getField(3), 74.0, 0.001);
		Assert.assertEquals((double) result.get(0).getField(4), 0.0, 0.001);
		Assert.assertEquals((double) result.get(0).getField(5), 1.0, 0.001);
		Assert.assertEquals(result.get(0).getField(6),
			VectorUtil.parseSparse("$16$1:3.0 2:4.0 4:7.0 9:3.0 10:4.0 12:7.0"));
	}

	@Test
	public void testBiVectorFunctionDense() {
		List <Row> df = new ArrayList <>();
		df.add(Row.of("1 2 3", "2 3 4"));
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec1 string, vec2 string");
		VectorBiFunction vecBiFunc = new VectorBiFunction()
			.setSelectedCols("vec1", "vec2")
			.setBiFuncName("minus")
			.setReservedCols("vec1", "vec2")
			.setOutputCol("vec_minus");

		BatchOperator <?> res = vecBiFunc.transform(data);

		vecBiFunc.setBiFuncName("plus").setOutputCol("vec_plus").setReservedCols("vec_minus", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.ElementWiseMultiply).setOutputCol("vec_multiply")
			.setReservedCols("vec_minus", "vec_plus", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.Dot).setOutputCol("vec_dot")
			.setReservedCols("vec_minus", "vec_plus", "vec_multiply", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.EuclidDistance).setOutputCol("vec_distance")
			.setReservedCols("vec_minus", "vec_plus", "vec_multiply", "vec_dot", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.Cosine).setOutputCol("vec_cos")
			.setReservedCols("vec_minus", "vec_plus", "vec_multiply", "vec_dot", "vec_distance", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.Merge).setOutputCol("vec_merge")
			.setReservedCols("vec_minus", "vec_plus", "vec_multiply", "vec_dot", "vec_distance", "vec_cos");
		List <Row> result = vecBiFunc.transform(res).collect();

		Assert.assertEquals(result.get(0).getField(0), VectorUtil.parseDense("-1.0 -1.0 -1.0"));
		Assert.assertEquals(result.get(0).getField(1), VectorUtil.parseDense("3 5 7"));
		Assert.assertEquals(result.get(0).getField(2), VectorUtil.parseDense("2 6 12"));
		Assert.assertEquals((double) result.get(0).getField(3), 20.0, 0.001);
		Assert.assertEquals((double) result.get(0).getField(4), Math.sqrt(3.0), 0.001);
		Assert.assertEquals((double) result.get(0).getField(5), 0.9925833339709303, 0.001);
		Assert.assertEquals(result.get(0).getField(6), VectorUtil.parseDense("1 2 3 2 3 4"));
	}

	@Test
	public void testBiVectorFunctionMixed1() {
		List <Row> df = new ArrayList <>();
		df.add(Row.of("1 2 3", "$3$0:2 1:3 2:4"));
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec1 string, vec2 string");

		VectorBiFunction vecBiFunc = new VectorBiFunction()
			.setSelectedCols("vec1", "vec2")
			.setBiFuncName("minus")
			.setReservedCols("vec1", "vec2")
			.setOutputCol("vec_minus");

		BatchOperator <?> res = vecBiFunc.transform(data);

		vecBiFunc.setBiFuncName("plus").setOutputCol("vec_plus").setReservedCols("vec_minus", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.ElementWiseMultiply).setOutputCol("vec_multiply")
			.setReservedCols("vec_minus", "vec_plus", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.Dot).setOutputCol("vec_dot")
			.setReservedCols("vec_minus", "vec_plus", "vec_multiply", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.EuclidDistance).setOutputCol("vec_distance")
			.setReservedCols("vec_minus", "vec_plus", "vec_multiply", "vec_dot", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.Cosine).setOutputCol("vec_cos")
			.setReservedCols("vec_minus", "vec_plus", "vec_multiply", "vec_dot", "vec_distance", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.Merge).setOutputCol("vec_merge")
			.setReservedCols("vec_minus", "vec_plus", "vec_multiply", "vec_dot", "vec_distance", "vec_cos");
		List <Row> result = vecBiFunc.transform(res).collect();

		Assert.assertEquals(result.get(0).getField(0), VectorUtil.parseDense("-1.0 -1.0 -1.0"));
		Assert.assertEquals(result.get(0).getField(1), VectorUtil.parseDense("3 5 7"));
		Assert.assertEquals(result.get(0).getField(2), VectorUtil.parseDense("2 6 12"));
		Assert.assertEquals((double) result.get(0).getField(3), 20.0, 0.001);
		Assert.assertEquals((double) result.get(0).getField(4), Math.sqrt(3.0), 0.001);
		Assert.assertEquals((double) result.get(0).getField(5), 0.9925833339709303, 0.001);
		Assert.assertEquals(result.get(0).getField(6),
			VectorUtil.parseSparse("$6$0:1.0 1:2.0 2:3.0 3:2.0 4:3.0 5:4.0"));
	}

	@Test
	public void testBiVectorFunctionMixed2() {
		List <Row> df = new ArrayList <>();
		df.add(Row.of("1 2 3", "$3$0:2 1:3 2:4"));
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec1 string, vec2 string");
		VectorBiFunction vecBiFunc = new VectorBiFunction()
			.setSelectedCols("vec1", "vec2")
			.setBiFuncName("minus")
			.setReservedCols("vec1", "vec2")
			.setOutputCol("vec_minus");

		BatchOperator <?> res = vecBiFunc.transform(data);

		vecBiFunc.setBiFuncName("plus").setOutputCol("vec_plus").setReservedCols("vec_minus", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.ElementWiseMultiply).setOutputCol("vec_multiply")
			.setReservedCols("vec_minus", "vec_plus", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.Dot).setOutputCol("vec_dot")
			.setReservedCols("vec_minus", "vec_plus", "vec_multiply", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.EuclidDistance).setOutputCol("vec_distance")
			.setReservedCols("vec_minus", "vec_plus", "vec_multiply", "vec_dot", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.Cosine).setOutputCol("vec_cos")
			.setReservedCols("vec_minus", "vec_plus", "vec_multiply", "vec_dot", "vec_distance", "vec1", "vec2");
		res = vecBiFunc.transform(res);

		vecBiFunc.setBiFuncName(BiFuncName.Merge).setOutputCol("vec_merge")
			.setReservedCols("vec_minus", "vec_plus", "vec_multiply", "vec_dot", "vec_distance", "vec_cos");
		List <Row> result = vecBiFunc.transform(res).collect();

		Assert.assertEquals(result.get(0).getField(0), VectorUtil.parseDense("-1.0 -1.0 -1.0"));
		Assert.assertEquals(result.get(0).getField(1), VectorUtil.parseDense("3 5 7"));
		Assert.assertEquals(result.get(0).getField(2), VectorUtil.parseDense("2 6 12"));
		Assert.assertEquals((double) result.get(0).getField(3), 20.0, 0.001);
		Assert.assertEquals((double) result.get(0).getField(4), Math.sqrt(3.0), 0.001);
		Assert.assertEquals((double) result.get(0).getField(5), 0.9925833339709303, 0.001);
		Assert.assertEquals(result.get(0).getField(6),
			VectorUtil.parseSparse("$6$0:1.0 1:2.0 2:3.0 3:2.0 4:3.0 5:4.0"));
	}
}