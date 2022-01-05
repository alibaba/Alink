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

public class ImputerTest extends AlinkTestBase {

	public static AlgoOperator getData(boolean isBatch) {

		Row[] testArray =
			new Row[] {
				Row.of("0", "a", 1L, 1, 2.0, true),
				Row.of("1", null, 2L, 2, -3.0, true),
				Row.of("2", "c", null, null, 2.0, false),
				Row.of("3", "a", 0L, 0, null, null),
			};

		String[] colNames = new String[] {"id", "f_string", "f_long", "f_int", "f_double", "f_boolean"};
		TableSchema schema = new TableSchema(
			colNames,
			new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.LONG, Types.INT, Types.DOUBLE, Types.BOOLEAN}
		);
		if (isBatch) {
			return new MemSourceBatchOp(Arrays.asList(testArray), schema);
		} else {
			return new MemSourceStreamOp(Arrays.asList(testArray), schema);
		}

	}

	@Test
	public void testPipelineMean() throws Exception {
		String[] selectedColNames = new String[] {"f_double", "f_long", "f_int"};

		testPipeline(selectedColNames);
	}

	public void testPipeline(String[] selectedColNames) throws Exception {
		BatchOperator batchData = (BatchOperator) getData(true);

		Imputer fillMissingValue = new Imputer()
			.setSelectedCols(selectedColNames)
			.setStrategy("value")
			.setFillValue("1");

		ImputerModel model = fillMissingValue.fit(batchData);
		BatchOperator res = model.transform(batchData);

		List <Row> rows = res.getDataSet().collect();
		rows.sort(new RowComparator(0));
		assertEquals(rows.get(0).getField(2), 1L);
		assertEquals(rows.get(1).getField(2), 2L);
		assertEquals(rows.get(2).getField(2), 1L);
		assertEquals(rows.get(3).getField(2), 0L);
		assertEquals(rows.get(0).getField(3), 1);
		assertEquals(rows.get(1).getField(3), 2);
		assertEquals(rows.get(2).getField(3), 1);
		assertEquals(rows.get(3).getField(3), 0);
		assertEquals((Double) rows.get(0).getField(4), 2.0, 0.001);
		assertEquals((Double) rows.get(1).getField(4), -3.0, 0.001);
		assertEquals((Double) rows.get(2).getField(4), 2.0, 0.001);
		assertEquals((Double) rows.get(3).getField(4), 1.0, 0.001);


		StreamOperator streamData = (StreamOperator) getData(false);
		CollectSinkStreamOp collectSinkStreamOp = new CollectSinkStreamOp().linkFrom(model.transform(streamData));
		StreamOperator.execute();
		List <Row> result = collectSinkStreamOp.getAndRemoveValues();
		result.sort(new RowComparator(0));
		assertEquals(result.get(0).getField(2), 1L);
		assertEquals(result.get(1).getField(2), 2L);
		assertEquals(result.get(2).getField(2), 1L);
		assertEquals(result.get(3).getField(2), 0L);
		assertEquals(result.get(0).getField(3), 1);
		assertEquals(result.get(1).getField(3), 2);
		assertEquals(result.get(2).getField(3), 1);
		assertEquals(result.get(3).getField(3), 0);
		assertEquals((Double) result.get(0).getField(4), 2.0, 0.001);
		assertEquals((Double) result.get(1).getField(4), -3.0, 0.001);
		assertEquals((Double) result.get(2).getField(4), 2.0, 0.001);
		assertEquals((Double) result.get(3).getField(4), 1.0, 0.001);
	}

}
