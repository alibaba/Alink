package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.MaxAbsScalerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.MaxAbsScalerTrainBatchOp;
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

public class MaxAbsScalerTest extends AlinkTestBase {
	public static AlgoOperator getMultiTypeData(boolean isBatch) {
		Row[] testArray =
			new Row[] {
				Row.of("0", 1.0, 2.0),
				Row.of("1", -1.0, -3.0),
				Row.of("2", 4.0, 2.0),
				Row.of("3", null, null),
			};
		String[] colNames = new String[] {"id", "f0", "f1"};
		TypeInformation[] colTypes = new TypeInformation[] {Types.STRING, Types.DOUBLE, Types.DOUBLE};
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
		List <Row> rows = res.getDataSet().collect();
		rows.sort(new RowComparator(0));
		assertEquals(rows.get(0).getField(1), 0.25);
		assertEquals(rows.get(0).getField(2), 0.6666666666666666);
		assertEquals(rows.get(1).getField(1), -0.25);
		assertEquals(rows.get(1).getField(2), -1.0);
		assertEquals(rows.get(2).getField(1), 1.0);
		assertEquals(rows.get(2).getField(2), 0.6666666666666666);
		assertEquals(rows.get(3).getField(1), null);
		assertEquals(rows.get(3).getField(2), null);

		CollectSinkStreamOp collectSinkStreamOp = new CollectSinkStreamOp().linkFrom(model.transform(streamData));
		StreamOperator.execute();
		List <Row> result = collectSinkStreamOp.getAndRemoveValues();
		result.sort(new RowComparator(0));
		assertEquals(rows.get(0).getField(1), 0.25);
		assertEquals(rows.get(0).getField(2), 0.6666666666666666);
		assertEquals(rows.get(1).getField(1), -0.25);
		assertEquals(rows.get(1).getField(2), -1.0);
		assertEquals(rows.get(2).getField(1), 1.0);
		assertEquals(rows.get(2).getField(2), 0.6666666666666666);
		assertEquals(rows.get(3).getField(1), null);
		assertEquals(rows.get(3).getField(2), null);
	}

	@Test
	public void testPipeline() throws Exception {
		testPipelineI();
	}

	@Test
	public void test() throws Exception {
		BatchOperator batchData = (BatchOperator) getMultiTypeData(true);
		String[] selectedColNames = new String[] {"f0", "f1"};
		MaxAbsScalerTrainBatchOp maxabs = new MaxAbsScalerTrainBatchOp()
			.setSelectedCols(selectedColNames).linkFrom(batchData);
		MaxAbsScalerPredictBatchOp pred = new MaxAbsScalerPredictBatchOp();
		pred.linkFrom(maxabs, batchData).print();
	}
}