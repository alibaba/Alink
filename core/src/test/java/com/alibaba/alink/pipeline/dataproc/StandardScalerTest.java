package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.dataproc.SortUtils;
import com.alibaba.alink.operator.common.dataproc.SortUtils.RowComparator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class StandardScalerTest extends AlinkTestBase {

	public static AlgoOperator getMultiTypeData(boolean isBatch) {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"0", "a", 1L, 1, 0.2, true}),
				Row.of(new Object[] {"1", null, 2L, 2, null, true}),
				Row.of(new Object[] {"2", "c", null, null, null, false}),
				Row.of(new Object[] {"3", "a", 0L, 0, null, null}),
			};

		String[] colNames = new String[] {"id", "f_string", "f_long", "f_int", "f_double", "f_boolean"};
		TypeInformation[] colTypes = new TypeInformation[] {Types.STRING, Types.STRING, Types.LONG, Types.INT,
			Types.DOUBLE,
			Types.BOOLEAN};
		TableSchema schema = new TableSchema(
			colNames,
			colTypes
		);

		if (isBatch) {
			return new MemSourceBatchOp(Arrays.asList(testArray), schema);
		} else {
			return new MemSourceStreamOp(Arrays.asList(testArray), schema);
		}
	}

	@Test
	public void test() throws Exception {
		BatchOperator batchData = (BatchOperator) getMultiTypeData(true);
		StreamOperator streamData = (StreamOperator) getMultiTypeData(false);
		String[] selectedColNames = new String[] {"f_long", "f_int", "f_double"};

		StandardScaler scaler = new StandardScaler()
			.setSelectedCols(selectedColNames)
			.setWithMean(true)
			.setWithStd(true);
		scaler.enableLazyPrintModelInfo();

		StandardScalerModel model = scaler.fit(batchData);
		BatchOperator res = model.transform(batchData);

		List <Row> rows = res.getDataSet().collect();
		rows.sort(new RowComparator(0));
		assertEquals(rows.get(0).getField(2), 0.0);
		assertEquals(rows.get(0).getField(3), 0.0);
		assertEquals(rows.get(0).getField(4), 0.0);
		assertEquals(rows.get(1).getField(2), 1.0);
		assertEquals(rows.get(1).getField(3), 1.0);
		assertEquals(rows.get(1).getField(4), null);
		assertEquals(rows.get(2).getField(2), null);
		assertEquals(rows.get(2).getField(3), null);
		assertEquals(rows.get(2).getField(4), null);
		assertEquals(rows.get(3).getField(2), -1.0);
		assertEquals(rows.get(3).getField(3), -1.0);
		assertEquals(rows.get(3).getField(4), null);

		CollectSinkStreamOp collectSinkStreamOp = new CollectSinkStreamOp().linkFrom(model.transform(streamData));
		StreamOperator.execute();
		rows = collectSinkStreamOp.getAndRemoveValues();
		rows.sort(new RowComparator(0));
		assertEquals(rows.get(0).getField(2), 0.0);
		assertEquals(rows.get(0).getField(3), 0.0);
		assertEquals(rows.get(0).getField(4), 0.0);
		assertEquals(rows.get(1).getField(2), 1.0);
		assertEquals(rows.get(1).getField(3), 1.0);
		assertEquals(rows.get(1).getField(4), null);
		assertEquals(rows.get(2).getField(2), null);
		assertEquals(rows.get(2).getField(3), null);
		assertEquals(rows.get(2).getField(4), null);
		assertEquals(rows.get(3).getField(2), -1.0);
		assertEquals(rows.get(3).getField(3), -1.0);
		assertEquals(rows.get(3).getField(4), null);
	}
}