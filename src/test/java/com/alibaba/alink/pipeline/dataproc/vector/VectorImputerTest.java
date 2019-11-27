package com.alibaba.alink.pipeline.dataproc.vector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VectorImputerTest {

	@Test
	public void testPipelineFillValue() throws Exception {
		Row[] rowData =
			new Row[] {
				Row.of("0", "1, 3, NaN"),
				Row.of("1", "0:-1.0 1:-3.0"),
				Row.of("2", "0:4.0 1:2.0")
			};

		String selectedColName = "vec";

		String strategy = "value";
		String fillValue = "-7.0";
		TableSchema schema = new TableSchema(
			new String[] {"id", "vec"},
			new TypeInformation<?>[] {Types.STRING, Types.STRING}
		);
		BatchOperator batchData = new MemSourceBatchOp(Arrays.asList(rowData), schema);
		StreamOperator streamData = new MemSourceStreamOp(Arrays.asList(rowData), schema);

		VectorImputer fillMissingValue = new VectorImputer()
				.setSelectedCol(selectedColName)
				.setStrategy(strategy)
				.setFillValue(fillValue);

		VectorImputerModel model = fillMissingValue.fit(batchData);

		BatchOperator res = model.transform(batchData);
		List rows = res.getDataSet().collect();
		HashMap<String, Vector> map = new HashMap<String, Vector>();
		map.put((String) ((Row) rows.get(0)).getField(0), VectorUtil.getVector(((Row) rows.get(0)).getField(1)));
		map.put((String) ((Row) rows.get(1)).getField(0), VectorUtil.getVector(((Row) rows.get(1)).getField(1)));
		map.put((String) ((Row) rows.get(2)).getField(0), VectorUtil.getVector(((Row) rows.get(2)).getField(1)));
		assertEquals(map.get("0"),
			VectorUtil.getVector("1.0 3.0 -7.0"));
		assertEquals(map.get("1"),
			VectorUtil.getVector("0:-1.0 1:-3.0"));
		assertEquals(map.get("2"),
			VectorUtil.getVector("0:4.0 1:2.0"));

		model.transform(streamData).print();
		StreamOperator.execute();
	}
}
