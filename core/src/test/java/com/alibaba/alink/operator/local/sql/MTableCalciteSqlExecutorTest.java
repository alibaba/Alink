package com.alibaba.alink.operator.local.sql;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.LocalMLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.sql.MTableCalciteSqlExecutor;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MTableCalciteSqlExecutorTest extends AlinkTestBase {
	MTable getIrisMTable() {
		return new MTable(IrisData.irisDoubleArray,
			new String[]{"sepal_length", "sepal_width", "petal_length", "petal_width", "category"});

		//int savedParallelism = MLEnvironmentFactory.getDefault().getExecutionEnvironment().getParallelism();
		//BatchOperator.setParallelism(1);
		//BatchOperator <?> source = new CsvSourceBatchOp()
		//	.setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
		//	.setSchemaStr(
		//		"sepal_length double, sepal_width double, petal_length double, petal_width double, category string");
		////BatchOperator <?> source = new MemSourceBatchOp(IrisData.irisDoubleArray,
		////		"sepal_length double, sepal_width double, petal_length double, petal_width double, category string");
		//MTable mTable = source.collectMTable();
		//BatchOperator.setParallelism(savedParallelism);
		//return mTable;
	}

	MTable getIrisMTable(int firstN) {
		return getIrisMTable().subTable(0, firstN);
		//int savedParallelism = MLEnvironmentFactory.getDefault().getExecutionEnvironment().getParallelism();
		//BatchOperator.setParallelism(1);
		//BatchOperator <?> source = new CsvSourceBatchOp()
		//	.setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
		//	.setSchemaStr(
		//		"sepal_length double, sepal_width double, petal_length double, petal_width double, category string")
		//	.firstN(firstN);
		//MTable mTable = source.collectMTable();
		//BatchOperator.setParallelism(savedParallelism);
		//return mTable;
	}

	@Test
	public void testSelect() {
		MTable input = getIrisMTable();
		MTableCalciteSqlExecutor sqlExecutor = new MTableCalciteSqlExecutor(LocalMLEnvironment.getInstance());
		MTable result = sqlExecutor.select(input, "category as a, sepal_length");
		Assert.assertEquals(input.getNumRow(), result.getNumRow());
		Assert.assertEquals(2, result.getNumCol());
		Assert.assertArrayEquals(new String[] {"a", "sepal_length"}, result.getColNames());
		for (int i = 0; i < input.getNumRow(); i += 1) {
			Assert.assertEquals(input.getRow(i).getField(4), result.getRow(i).getField(0));
			Assert.assertEquals(input.getRow(i).getField(0), result.getRow(i).getField(1));
		}
	}

	@Test
	public void testAs() {
		MTable input = getIrisMTable();
		MTableCalciteSqlExecutor sqlExecutor = new MTableCalciteSqlExecutor(LocalMLEnvironment.getInstance());
		MTable result = sqlExecutor.as(input, "a, b, c, \"d\", `id`");
		Assert.assertEquals(input.getNumRow(), result.getNumRow());
		Assert.assertEquals(5, result.getNumCol());
		Assert.assertArrayEquals(new String[] {"a", "b", "c", "d", "id"}, result.getColNames());
		for (int i = 0; i < input.getNumRow(); i += 1) {
			for (int j = 0; j < input.getNumCol(); j += 1) {
				Assert.assertEquals(input.getRow(i).getField(j), result.getRow(i).getField(j));
			}
		}
	}

	@Test
	public void testWhere() {
		MTable input = getIrisMTable();
		MTableCalciteSqlExecutor sqlExecutor = new MTableCalciteSqlExecutor(LocalMLEnvironment.getInstance());
		MTable result = sqlExecutor.where(input, "sepal_length > 5.9 and category = 'Iris-virginica'");
		Assert.assertEquals(input.getNumCol(), result.getNumCol());
		for (int i = 0; i < result.getNumRow(); i += 1) {
			Assert.assertTrue((Double) result.getRow(i).getField(0) > 5.9);
			Assert.assertEquals("Iris-virginica", result.getRow(i).getField(4));
		}
	}

	@Test
	public void testDistinct() {
		MTable input = getIrisMTable();
		MTableCalciteSqlExecutor sqlExecutor = new MTableCalciteSqlExecutor(LocalMLEnvironment.getInstance());
		MTable result = sqlExecutor.distinct(sqlExecutor.select(input, "category"));
		Assert.assertEquals(1, result.getNumCol());
		Assert.assertEquals(3, result.getNumRow());
	}

	@Test
	public void testOrderBy() {
		MTable input = getIrisMTable();
		MTableCalciteSqlExecutor sqlExecutor = new MTableCalciteSqlExecutor(LocalMLEnvironment.getInstance());
		MTable result = sqlExecutor.orderBy(input, "petal_length", true, 10);
		Assert.assertEquals(input.getNumCol(), result.getNumCol());
		Assert.assertEquals(10, result.getNumRow());
		double last = -Double.MAX_VALUE;
		for (int i = 0; i < result.getNumRow(); i += 1) {
			Assert.assertTrue((Double) result.getRow(i).getField(2) >= last);
			last = (Double) result.getRow(i).getField(2);
		}
	}

	@Test
	public void testGroupBy() {
		MTable input = getIrisMTable();
		MTableCalciteSqlExecutor sqlExecutor = new MTableCalciteSqlExecutor(LocalMLEnvironment.getInstance());
		MTable result = sqlExecutor.groupBy(input, "category",
			"category, count(*) as cnt, max(sepal_length) as max_sepal_length");
		Assert.assertEquals(3, result.getNumCol());
		Assert.assertEquals(3, result.getNumRow());
		Map <String, Double> maxSepalLength = new HashMap <>();
		maxSepalLength.put("Iris-setosa", 5.8);
		maxSepalLength.put("Iris-versicolor", 7.0);
		maxSepalLength.put("Iris-virginica", 7.9);
		for (int i = 0; i < result.getNumRow(); i += 1) {
			Row row = result.getRow(i);
			Assert.assertEquals(maxSepalLength.get((String) row.getField(0)), row.getField(2));
			Assert.assertEquals(50L, row.getField(1));
		}
	}

	@Test
	public void testJoin() {
		MTable input = getIrisMTable();
		MTableCalciteSqlExecutor sqlExecutor = new MTableCalciteSqlExecutor(LocalMLEnvironment.getInstance());
		MTable categoryCount = sqlExecutor.groupBy(input, "category",
			"category, count(*) as cnt, max(sepal_length) as max_sepal_length");
		MTable result = sqlExecutor.join(input, categoryCount, "a.category = b.category",
			"a.category, a.sepal_length, b.cnt, b.max_sepal_length");
		Assert.assertEquals(4, result.getNumCol());
		Assert.assertEquals(input.getNumRow(), result.getNumRow());
		Map <String, Double> maxSepalLength = new HashMap <>();
		maxSepalLength.put("Iris-setosa", 5.8);
		maxSepalLength.put("Iris-versicolor", 7.0);
		maxSepalLength.put("Iris-virginica", 7.9);
		for (int i = 0; i < result.getNumRow(); i += 1) {
			Row row = result.getRow(i);
			Assert.assertEquals(maxSepalLength.get((String) row.getField(0)), row.getField(3));
		}
	}

	@Test
	public void testIntersect() {
		MTable input = getIrisMTable();
		MTable input2 = getIrisMTable();
		MTableCalciteSqlExecutor sqlExecutor = new MTableCalciteSqlExecutor(LocalMLEnvironment.getInstance());
		MTable result = sqlExecutor.intersect(input, input2);
		Assert.assertEquals(input.getNumCol(), result.getNumCol());
		Assert.assertEquals(147, result.getNumRow());
	}

	@Test(expected = AkUnclassifiedErrorException.class)
	public void testIntersectAll() {
		// TODO: make intersectAll work
		MTable input = getIrisMTable();
		MTable input2 = getIrisMTable(50);
		MTableCalciteSqlExecutor sqlExecutor = new MTableCalciteSqlExecutor(LocalMLEnvironment.getInstance());
		MTable result = sqlExecutor.intersectAll(input, input2);
		Assert.assertEquals(input.getNumCol(), result.getNumCol());
		Assert.assertEquals(50, result.getNumRow());
	}

	@Test
	public void testUnion() {
		MTable input = getIrisMTable();
		MTable input2 = getIrisMTable();
		MTableCalciteSqlExecutor sqlExecutor = new MTableCalciteSqlExecutor(LocalMLEnvironment.getInstance());
		MTable result = sqlExecutor.union(input, input2);
		Assert.assertEquals(input.getNumCol(), result.getNumCol());
		Assert.assertEquals(147, result.getNumRow());
	}

	@Test
	public void testUnionAll() {
		MTable input = getIrisMTable();
		MTable input2 = getIrisMTable();
		MTableCalciteSqlExecutor sqlExecutor = new MTableCalciteSqlExecutor(LocalMLEnvironment.getInstance());
		MTable result = sqlExecutor.unionAll(input, input2);
		Assert.assertEquals(input.getNumCol(), result.getNumCol());
		Assert.assertEquals(300, result.getNumRow());
	}

	@Test
	public void testMinus() {
		MTable input = getIrisMTable();
		MTable input2 = getIrisMTable(50);
		MTableCalciteSqlExecutor sqlExecutor = new MTableCalciteSqlExecutor(LocalMLEnvironment.getInstance());
		MTable result = sqlExecutor.minus(input, input2);
		Assert.assertEquals(input.getNumCol(), result.getNumCol());
		Assert.assertEquals(99, result.getNumRow());
	}

	@Test(expected = AkUnclassifiedErrorException.class)
	public void testMinusAll() {
		// TODO: make minusAll work
		MTable input = getIrisMTable();
		MTable input2 = getIrisMTable(50);
		MTableCalciteSqlExecutor sqlExecutor = new MTableCalciteSqlExecutor(LocalMLEnvironment.getInstance());
		MTable result = sqlExecutor.minusAll(input, input2);
		Assert.assertEquals(input.getNumCol(), result.getNumCol());
		Assert.assertEquals(100, result.getNumRow());
	}

	@Test
	public void testUDTs() {
		List <Row> df = Arrays.asList(
			Row.of("Ohio", 2000, 1.5, new DenseVector(new double[] {1., 2.}), new MTable(new Object[] {"1"}, "id")),
			Row.of("Ohio", 2001, 1.7, new DenseVector(new double[] {1., 2.}), new MTable(new Object[] {"1"}, "id")),
			Row.of("Ohio", 2002, 3.6, new DenseVector(new double[] {1., 2.}), new MTable(new Object[] {"1"}, "id")),
			Row.of("Nevada", 2001, 2.4, new DenseVector(new double[] {1., 2.}), new MTable(new Object[] {"1"}, "id")),
			Row.of("Nevada", 2002, 2.9, new DenseVector(new double[] {1., 2.}), new MTable(new Object[] {"1"}, "id")),
			Row.of("Nevada", 2003, 3.2, new DenseVector(new double[] {1., 2.}), new MTable(new Object[] {"1"}, "id"))
		);
		MTable mt = new MTable(df, "f1 string, f2 int, f3 double, f4 dense_vector, f5 mtable");
		MTableCalciteSqlExecutor sqlExecutor = new MTableCalciteSqlExecutor(LocalMLEnvironment.getInstance());
		MTable result = sqlExecutor.select(mt, "f1, f4, f5");
		Assert.assertEquals(mt.getNumRow(), result.getNumRow());
		Assert.assertEquals(AlinkTypes.STRING, result.getColTypes()[0]);
		Assert.assertEquals(AlinkTypes.DENSE_VECTOR, result.getColTypes()[1]);
		Assert.assertEquals(AlinkTypes.M_TABLE, result.getColTypes()[2]);
	}

	@Test
	public void testUTF8Charset() {
		List <Row> df = Arrays.asList(
			Row.of("昨天", 2000),
			Row.of("今天", 2001),
			Row.of("明天", 2002),
			Row.of("昨天", 2001),
			Row.of("今天", 2002),
			Row.of("明天", 2003)
		);
		MTable input = new MTable(df, "f1 string, f2 int");
		MTableCalciteSqlExecutor sqlExecutor = new MTableCalciteSqlExecutor(LocalMLEnvironment.getInstance());
		MTable result = sqlExecutor.where(input, "f2 > 2001 and f1 <> '昨天'");
		Assert.assertEquals(input.getNumCol(), result.getNumCol());
	}
}
