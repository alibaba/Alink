package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class SqlQueryBatchTest extends AlinkTestBase {

	public static class PlusOne extends ScalarFunction {
		private static final long serialVersionUID = 4481034882059403584L;

		public double eval(double x) {
			return x + 1;
		}
	}

	public static class SplitOp extends TableFunction <Row> {
		private static final long serialVersionUID = -10056574593918617L;

		public void eval(Object... values) {
			long counter = 0;
			for (Object value : values) {
				collector.collect(Row.of(counter, value));
				counter += 1;
			}
		}

		@Override
		public TypeInformation getResultType() {
			return Types.ROW(Types.LONG(), Types.DOUBLE());
		}
	}

	private BatchOperator data;
	private StreamOperator data_stream;

	@Before
	public void setup() throws Exception {
		Row[] rows =
			new Row[] {
				Row.of(new Object[] {"1", 1, 1.1, 1.0, true}),
				Row.of(new Object[] {"2", -2, 0.9, 2.0, false}),
				Row.of(new Object[] {"3", 100, -0.01, 3.0, true}),
				Row.of(new Object[] {"4", -99, null, 4.0, false}),
				Row.of(new Object[] {"5", 1, 1.1, 5.0, true}),
				Row.of(new Object[] {"6", -2, 0.9, 6.0, false})
			};
		String[] colnames = new String[] {"f1", "f2", "f3", "f4", "f5"};
		data = new MemSourceBatchOp(Arrays.asList(rows), colnames);
		data_stream = new MemSourceStreamOp(Arrays.asList(rows), colnames);
	}

	@Test
	public void testBatchOpSqlQuery() throws Exception {
		String tableName1 = RandomStringUtils.randomAlphabetic(12);
		String tableName2 = RandomStringUtils.randomAlphabetic(12);
		data.registerTableName(tableName1);
		data.registerTableName(tableName2);
		BatchOperator.sqlQuery(
			String.format("select a.f1,b.f2 from %s as a join %s as b on a.f1=b.f1", tableName1, tableName2)).print();
	}

	@Test
	public void testBatchRegisterUDF() throws Exception {
		String tableName = RandomStringUtils.randomAlphabetic(12);
		data.registerTableName(tableName);
		BatchOperator.registerFunction("f", new PlusOne());
		BatchOperator.sqlQuery("select f(f4) from " + tableName).print();
	}

	@Test
	public void testBatchRegisterUDTF() throws Exception {
		String tableName = RandomStringUtils.randomAlphabetic(12);
		data.registerTableName(tableName);
		BatchOperator.registerFunction("split", new SplitOp());
		BatchOperator.sqlQuery(
			String.format("select f3, index, v from %s, lateral table(split(f3, f4)) as T(index, v)", tableName))
			.print();
	}

	@Test
	public void testStreamRegisterUDF() throws Exception {
		String tableName = RandomStringUtils.randomAlphabetic(12);
		data_stream.registerTableName(tableName);
		StreamOperator.registerFunction("f", new PlusOne());
		StreamOperator.sqlQuery("select f(f4) as e from " + tableName).print();
		StreamOperator.execute();
	}

	@Test
	public void testStreamRegisterUDTF() throws Exception {
		String tableName = RandomStringUtils.randomAlphabetic(12);
		data_stream.registerTableName(tableName);
		StreamOperator.registerFunction("split", new SplitOp());
		StreamOperator.sqlQuery(
			String.format("select f3, index, v from %s, lateral table(split(f3, f4)) as T(index, v)", tableName))
			.print();
		StreamOperator.execute();
	}
}
