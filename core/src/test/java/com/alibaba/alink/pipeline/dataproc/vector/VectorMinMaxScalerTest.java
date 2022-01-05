package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
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

public class VectorMinMaxScalerTest extends AlinkTestBase {

	@Test
	public void testPipeline() throws Exception {
		Row[] rowData =
			new Row[] {
				Row.of("0", "1.0 2.0"),
				Row.of("1", "-1.0 -3.0"),
				Row.of("2", "4.0 2.0"),
			};
		TableSchema schema = new TableSchema(
			new String[] {"id", "vec"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING}
		);
		BatchOperator batchData = new MemSourceBatchOp(Arrays.asList(rowData), schema);
		StreamOperator streamData = new MemSourceStreamOp(Arrays.asList(rowData), schema);

		String selectedColName = "vec";

		VectorMinMaxScaler scaler = new VectorMinMaxScaler()
			.setSelectedCol(selectedColName);

		scaler.setMax(2.0);
		scaler.setMin(-3.0);

		VectorMinMaxScalerModel model = scaler.fit(batchData);
		BatchOperator res = model.transform(batchData);
		List <Row> rows = res.getDataSet().collect();
		rows.sort(new RowComparator(0));
		assertEquals(rows.get(0).getField(1), VectorUtil.getVector("-1.0 2.0"));
		assertEquals(rows.get(1).getField(1), VectorUtil.getVector("-3.0 -3.0"));
		assertEquals(rows.get(2).getField(1), VectorUtil.getVector("2.0 2.0"));

		CollectSinkStreamOp collectSinkStreamOp = new CollectSinkStreamOp()
			.linkFrom(model.transform(streamData));
		StreamOperator.execute();
		List <Row> result = collectSinkStreamOp.getAndRemoveValues();
		result.sort(new RowComparator(0));
		assertEquals(VectorUtil.getVector(result.get(0).getField(1)), VectorUtil.getVector("-1.0 2.0"));
		assertEquals(VectorUtil.getVector(result.get(1).getField(1)), VectorUtil.getVector("-3.0 -3.0"));
		assertEquals(VectorUtil.getVector(result.get(2).getField(1)), VectorUtil.getVector("2.0 2.0"));
	}

}
