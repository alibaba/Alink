package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.dataproc.SortUtils;
import com.alibaba.alink.operator.common.dataproc.SortUtils.RowComparator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.TestUtil;
import com.alibaba.alink.pipeline.dataproc.GenerateData;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class VectorMaxAbsScalerTest extends AlinkTestBase {

	private static void testPipelineI() throws Exception {

		Row[] rowData =
			new Row[] {
				Row.of("0", "1.0 2.0"),
				Row.of("1", "-1.0 -4.0"),
				Row.of("2", "-4.0 3.0"),
			};
		TableSchema schema = new TableSchema(
			new String[] {"id", "vec"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING}
		);
		BatchOperator batchData = new MemSourceBatchOp(Arrays.asList(rowData), schema);
		StreamOperator streamData = new MemSourceStreamOp(Arrays.asList(rowData), schema);

		String selectedColName = "vec";

		VectorMaxAbsScaler scaler = new VectorMaxAbsScaler()
			.setSelectedCol(selectedColName);

		VectorMaxAbsScalerModel model = scaler.fit(batchData);
		BatchOperator batchRes = model.transform(batchData);
		List <Row> batchList = batchRes.getDataSet().collect();
		batchList.sort(new RowComparator(0));
		assertEquals(batchList.get(0).getField(1), VectorUtil.getVector("0.25 0.5"));
		assertEquals(batchList.get(1).getField(1), VectorUtil.getVector("-0.25 -1.0"));
		assertEquals(batchList.get(2).getField(1), VectorUtil.getVector("-1.0 0.75"));

		CollectSinkStreamOp collectSinkStreamOp = new CollectSinkStreamOp()
			.linkFrom(model.transform(streamData));
		StreamOperator.execute();
		List <Row> result = collectSinkStreamOp.getAndRemoveValues();
		result.sort(new RowComparator(0));
		assertEquals(VectorUtil.getVector(result.get(0).getField(1)), VectorUtil.getVector("0.25 0.5"));
		assertEquals(VectorUtil.getVector(result.get(1).getField(1)), VectorUtil.getVector("-0.25 -1.0"));
		assertEquals(VectorUtil.getVector(result.get(2).getField(1)), VectorUtil.getVector("-1.0 0.75"));
	}

	@Test
	public void testPipeline() throws Exception {
		testPipelineI();
	}

}