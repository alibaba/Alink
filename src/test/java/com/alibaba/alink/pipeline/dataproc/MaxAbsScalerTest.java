package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MaxAbsScalerTest {
	public static AlgoOperator getMultiTypeData(boolean isBatch) {
		Row[] testArray =
			new Row[]{
				Row.of("0", 1.0, 2.0),
				Row.of("1", -1.0, -3.0),
				Row.of("2", 4.0, 2.0),
				Row.of("3", null, null),
			};
		String[] colNames = new String[]{"id", "f0", "f1"};
		TypeInformation[] colTypes = new TypeInformation[]{Types.STRING, Types.DOUBLE, Types.DOUBLE};
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
	private static void testPipelineI() throws Exception {

		BatchOperator batchData = (BatchOperator) getMultiTypeData(true);
		StreamOperator streamData = (StreamOperator) getMultiTypeData(false);

		String[] selectedColNames = new String[] {"f0", "f1"};

		MaxAbsScaler scaler3 = new MaxAbsScaler()
			.setSelectedCols(selectedColNames);

		MaxAbsScalerModel model = scaler3.fit(batchData);

		BatchOperator res = model.transform(batchData);
		List rows = res.getDataSet().collect();
		HashMap<String, Tuple2<Double, Double>> map = new HashMap<String, Tuple2<Double, Double>>();
		map.put((String) ((Row) rows.get(0)).getField(0), Tuple2.of(
			(Double) ((Row) rows.get(0)).getField(1),
			(Double) ((Row) rows.get(0)).getField(2)));
		map.put((String) ((Row) rows.get(1)).getField(0), Tuple2.of(
			(Double) ((Row) rows.get(1)).getField(1),
			(Double) ((Row) rows.get(1)).getField(2)));
		map.put((String) ((Row) rows.get(2)).getField(0), Tuple2.of(
			(Double) ((Row) rows.get(2)).getField(1),
			(Double) ((Row) rows.get(2)).getField(2)));
		map.put((String) ((Row) rows.get(3)).getField(0), Tuple2.of(
			(Double) ((Row) rows.get(3)).getField(1),
			(Double) ((Row) rows.get(3)).getField(2)));
		assertEquals(map.get("0"), new Tuple2<>(0.25, 0.6666666666666666));
		assertEquals(map.get("1"), new Tuple2<>(-0.25, -1.0));
		assertEquals(map.get("2"), new Tuple2<>(1.0, 0.6666666666666666));
		assertEquals(map.get("3"), new Tuple2<>(null, null));

		model.transform(streamData).print();
		StreamOperator.execute();
	}

	@Test
	public void testPipeline() throws Exception {
		testPipelineI();
	}
}