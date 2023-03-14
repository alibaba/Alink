package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.shared.HasFuncName.FuncName;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class VectorFunctionTest extends AlinkTestBase {
	@Test
	public void testVectorFunctionSparse() {
		List <Row> df = new ArrayList <>();
		df.add(Row.of("$8$1:3,2:4,4:7", "$8$1:3,2:4,4:7"));
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec1 string, vec2 string");
		VectorFunction vecFunc = new VectorFunction()
			.setSelectedCol("vec1")
			.setFuncName("normL2")
			.setReservedCols("vec1")
			.setOutputCol("vec_normL2");
		BatchOperator <?> res = vecFunc.transform(data);

		vecFunc.setFuncName("scale").setOutputCol("vec_scale")
			.setReservedCols("vec_normL2", "vec1")
			.setWithVariable(2);
		res = vecFunc.transform(res);

		vecFunc.setFuncName(FuncName.Normalize).setOutputCol("vec_normalize").setWithVariable(4)
			.setReservedCols("vec_normL2","vec_scale", "vec1");
		res = vecFunc.transform(res);

		vecFunc.setFuncName(FuncName.NormL1).setOutputCol("vec_normL1")
			.setReservedCols("vec_normL2","vec_scale", "vec_normalize", "vec1");
		res = vecFunc.transform(res);

		vecFunc.setFuncName(FuncName.ArgMax).setOutputCol("vec_argMax")
			.setReservedCols("vec_normL2","vec_scale", "vec_normalize", "vec_normL1", "vec1");
		res = vecFunc.transform(res);

		vecFunc.setFuncName(FuncName.Max).setOutputCol("vec_max")
			.setReservedCols("vec_normL2","vec_scale", "vec_normalize", "vec_normL1", "vec_argMax", "vec1");
		res = vecFunc.transform(res);

		vecFunc.setFuncName(FuncName.NormL2Square).setOutputCol("vec_normL2Square")
			.setReservedCols("vec_normL2","vec_scale", "vec_normalize", "vec_normL1", "vec_argMax", "vec_max");
		List<Row> result = vecFunc.transform(res).collect();

		Assert.assertEquals((double)result.get(0).getField(0), 8.602325267042627, 0.001);
		Assert.assertEquals(result.get(0).getField(1), VectorUtil.parseSparse("$8$1:6.0 2:8.0 4:14.0"));
		Assert.assertEquals(result.get(0).getField(2),
			VectorUtil.parseSparse("$8$1:0.4147275572892476 2:0.5529700763856634 4:0.967697633674911"));
		Assert.assertEquals((double) result.get(0).getField(3), 14.0, 0.001);
		Assert.assertEquals(result.get(0).getField(4), "4");
		Assert.assertEquals(result.get(0).getField(5), "7.0");
		Assert.assertEquals((double) result.get(0).getField(6), 74.0, 0.001);
	}

	@Test
	public void testVectorFunctionDense() {
		List <Row> df = new ArrayList <>();
		df.add(Row.of("3 4 7", "2 3 4"));
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec1 string, vec2 string");
		VectorFunction vecFunc = new VectorFunction()
			.setSelectedCol("vec1")
			.setFuncName("normL2")
			.setReservedCols("vec1")
			.setOutputCol("vec_normL2");
		BatchOperator <?> res = vecFunc.transform(data);

		vecFunc.setFuncName("scale").setOutputCol("vec_scale")
			.setReservedCols("vec_normL2", "vec1")
			.setWithVariable(2);
		res = vecFunc.transform(res);

		vecFunc.setFuncName(FuncName.Normalize).setOutputCol("vec_normalize").setWithVariable(4)
			.setReservedCols("vec_normL2","vec_scale", "vec1");
		res = vecFunc.transform(res);

		vecFunc.setFuncName(FuncName.NormL1).setOutputCol("vec_normL1")
			.setReservedCols("vec_normL2","vec_scale", "vec_normalize", "vec1");
		res = vecFunc.transform(res);

		vecFunc.setFuncName(FuncName.ArgMax).setOutputCol("vec_argMax")
			.setReservedCols("vec_normL2","vec_scale", "vec_normalize", "vec_normL1", "vec1");
		res = vecFunc.transform(res);

		vecFunc.setFuncName(FuncName.Max).setOutputCol("vec_max")
			.setReservedCols("vec_normL2","vec_scale", "vec_normalize", "vec_normL1", "vec_argMax", "vec1");
		res = vecFunc.transform(res);

		vecFunc.setFuncName(FuncName.NormL2Square).setOutputCol("vec_normL2Square")
			.setReservedCols("vec_normL2","vec_scale", "vec_normalize", "vec_normL1", "vec_argMax", "vec_max");
		List<Row> result = vecFunc.transform(res).collect();

		Assert.assertEquals((double)result.get(0).getField(0), 8.602325267042627, 0.001);
		Assert.assertEquals(result.get(0).getField(1), VectorUtil.parseDense("6.0 8.0 14.0"));
		Assert.assertEquals(result.get(0).getField(2),
			VectorUtil.parseDense("0.4147275572892476 0.5529700763856634 0.967697633674911"));
		Assert.assertEquals((double) result.get(0).getField(3), 14.0, 0.001);
		Assert.assertEquals(result.get(0).getField(4), "2");
		Assert.assertEquals(result.get(0).getField(5), "7.0");
		Assert.assertEquals((double) result.get(0).getField(6), 74.0, 0.001);
	}
}