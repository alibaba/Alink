package com.alibaba.alink.operator.common.sql;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;

public class TimeSeriesAggTest extends AlinkTestBase {

	@Test
	public void testDouble() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"a", new Timestamp(11), 1.3}),
				Row.of(new Object[] {"b", new Timestamp(12), -2.5}),
				Row.of(new Object[] {"c", new Timestamp(13), 100.2}),
				Row.of(new Object[] {"d", new Timestamp(14), -99.9}),
				Row.of(new Object[] {"a", new Timestamp(15), 1.4}),
				Row.of(new Object[] {"b", new Timestamp(16), -2.2}),
				Row.of(new Object[] {"c", new Timestamp(17), 100.9}),
				Row.of(new Object[] {"d", new Timestamp(18), -99.5})
			};
		String[] colNames = new String[] {"id", "time1", "f0"};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		inOp.groupBy("id", "id, timeseries_agg(time1, f0) as data")
			.print();

	}

	@Test
	public void testVector() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"a", new Timestamp(11), new DenseVector(new double[] {1.3, 1.4})}),
				Row.of(new Object[] {"b", new Timestamp(12), new DenseVector(new double[] {-2.5, 1.4})}),
				Row.of(new Object[] {"c", new Timestamp(13), new DenseVector(new double[] {100.2, 1.4})}),
				Row.of(new Object[] {"d", new Timestamp(14), new DenseVector(new double[] {-99.9, 1.4})}),
				Row.of(new Object[] {"a", new Timestamp(15), new DenseVector(new double[] {1.4, 1.4})}),
				Row.of(new Object[] {"b", new Timestamp(16), new DenseVector(new double[] {-2.2, 1.4})}),
				Row.of(new Object[] {"c", new Timestamp(17), new DenseVector(new double[] {100.9, 1.4})}),
				Row.of(new Object[] {"d", new Timestamp(18), new DenseVector(new double[] {-99.5, 1.4})})
			};
		String[] colNames = new String[] {"id", "time1", "f0"};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		inOp.groupBy("id", "id, timeseries_agg(time1, f0) as data")
			.print();

	}

}