//package com.alibaba.alink.operator.local.sql;
//
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.table.api.Types;
//import org.apache.flink.table.functions.ScalarFunction;
//import org.apache.flink.table.functions.TableFunction;
//import org.apache.flink.types.Row;
//
//import com.alibaba.alink.common.MTable;
//import com.alibaba.alink.operator.local.LocalOperator;
//import com.alibaba.alink.operator.local.source.TableSourceLocalOp;
//import org.apache.commons.lang3.RandomStringUtils;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import java.util.Arrays;
//
//public class LocalOpSqlQueryTest {
//
//	public static class PlusOne extends ScalarFunction {
//		private static final long serialVersionUID = 4481034882059403584L;
//
//		public double eval(double x) {
//			return x + 1;
//		}
//	}
//
//	public static class SplitOp extends TableFunction <Row> {
//		private static final long serialVersionUID = -10056574593918617L;
//
//		public void eval(Object... values) {
//			long counter = 0;
//			for (Object value : values) {
//				collect(Row.of(counter, value));
//				counter += 1;
//			}
//		}
//
//		@Override
//		public TypeInformation <Row> getResultType() {
//			//noinspection deprecation
//			return Types.ROW(Types.LONG(), Types.DOUBLE());
//		}
//	}
//
//	public static class SplitOp4 extends TableFunction <Row> {
//		private static final long serialVersionUID = -10056574593918617L;
//
//		public void eval(Object value, Object value2, Object value3, Object value4) {
//			long counter = 0;
//			//for (Object value : values) {
//			collect(Row.of(counter, value));
//			counter += 1;
//			collect(Row.of(counter, value2));
//			counter += 1;
//			collect(Row.of(counter, value3));
//			counter += 1;
//			collect(Row.of(counter, value4));
//			//}
//		}
//
//		@Override
//		public TypeInformation <Row> getResultType() {
//			//noinspection deprecation
//			return Types.ROW(Types.LONG(), Types.DOUBLE());
//		}
//	}
//
//	@BeforeClass
//	public static void beforeClass() throws Exception {
//		LocalOperator.registerFunction("split", new SplitOp());
//		LocalOperator.registerFunction("split4", new SplitOp4());
//	}
//
//	private LocalOperator <?> data;
//
//	@Before
//	public void setup() throws Exception {
//		Row[] rows =
//			new Row[] {
//				Row.of("1", 1, 1.1, 1.0, true),
//				Row.of("2", -2, 0.9, 2.0, false),
//				Row.of("3", 100, -0.01, 3.0, true),
//				Row.of("4", -99, null, 4.0, false),
//				Row.of("5", 1, 1.1, 5.0, true),
//				Row.of("6", -2, 0.9, 6.0, false)
//			};
//		String[] colnames = new String[] {"f1", "f2", "f3", "f4", "f5"};
//		data = new TableSourceLocalOp(new MTable(Arrays.asList(rows), colnames));
//	}
//
//	@Test
//	public void testLocalOpSqlQuery() {
//		String tableName1 = RandomStringUtils.randomAlphabetic(12);
//		String tableName2 = RandomStringUtils.randomAlphabetic(12);
//		data.registerTableName(tableName1);
//		data.registerTableName(tableName2);
//		LocalOperator.sqlQuery(
//			String.format("select a.f1,b.f2 from %s as a join %s as b on a.f1=b.f1", tableName1, tableName2)).print();
//	}
//
//	@Test()
//	public void testNormalUdf() {
//		String tableName = RandomStringUtils.randomAlphabetic(12);
//		data.registerTableName(tableName);
//		LocalOperator.registerFunction("f", new PlusOne());
//		LocalOperator.sqlQuery("select f(f4) from " + tableName).print();
//	}
//
//	@Test
//	public void testNormalUdtf() {
//		String tableName = RandomStringUtils.randomAlphabetic(12);
//		data.registerTableName(tableName);
//		LocalOperator <?> result = LocalOperator.sqlQuery(
//			String.format("select f3, index, v from %s, lateral table(split4(f3, f4, f3, f4)) as T(index, v)",
//				tableName));
//		Assert.assertEquals(4 * data.getOutputTable().getNumRow(), result.getOutputTable().getNumRow());
//		Assert.assertEquals(3, result.getOutputTable().getNumCol());
//		result.print();
//	}
//
//	@Test
//	public void testVarargUdtf() {
//		String tableName = RandomStringUtils.randomAlphabetic(12);
//		data.registerTableName(tableName);
//		LocalOperator <?> result = LocalOperator.sqlQuery(
//			String.format("select f3, index, v from %s, lateral table(split(f3, f4)) as T(index, v)",
//				tableName));
//		Assert.assertEquals(2 * data.getOutputTable().getNumRow(), result.getOutputTable().getNumRow());
//		Assert.assertEquals(3, result.getOutputTable().getNumCol());
//		result.print();
//	}
//}
