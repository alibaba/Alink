package com.alibaba.alink.pipeline.dataproc.vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class VectorInteractionTest {

	AlgoOperator getData(boolean isBatch) {
		TableSchema schema = new TableSchema(
			new String[] {"id", "c0", "c1", "c2", "c3"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING}
		);

		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(new Object[] {"0", "$6$1:2.0 2:3.0 5:4.3", "3.0 2.0 3.0", "1 4 6 8", "$6$1:2.0 2:3.0 5:4.3"}));
		rows.add(Row.of(new Object[] {"1", "$8$1:2.0 2:3.0 7:4.3", "3.0 2.0 3.0", "1 4 6 8", "$6$1:2.0 2:3.0 5:4.3"}));
		rows.add(Row.of(new Object[] {"2", "$8$1:2.0 2:3.0 7:4.3", "2.0 3.0", "1 4 6 8", "$6$1:2.0 2:3.0 5:4.3"}));

		if (isBatch) {
			return new MemSourceBatchOp(rows, schema);
		} else {
			return new MemSourceStreamOp(rows, schema);
		}
	}

	@Test
	public void pipelineBatchTest() throws Exception {
		BatchOperator res = new VectorInteraction().setSelectedCols(new String[] {"c0", "c3"})
			.setOutputCol("product_result").transform((BatchOperator)getData(true));
		List rows = res.getDataSet().collect();
		HashMap<String, Vector> map = new HashMap<String, Vector>();
		map.put((String) ((Row) rows.get(0)).getField(0), VectorUtil.getVector(((Row) rows.get(0)).getField(5)));
		map.put((String) ((Row) rows.get(1)).getField(0), VectorUtil.getVector(((Row) rows.get(1)).getField(5)));
		map.put((String) ((Row) rows.get(2)).getField(0), VectorUtil.getVector(((Row) rows.get(2)).getField(5)));
		assertEquals(map.get("0"),
			VectorUtil.getVector("$36$7:4.0 8:6.0 11:8.6 13:6.0 14:9.0 17:12.899999999999999 31:8.6 32:12.899999999999999 35:18.49"));
		assertEquals(map.get("1"),
			VectorUtil.getVector("$48$9:4.0 10:6.0 15:8.6 17:6.0 18:9.0 23:12.899999999999999 41:8.6 42:12.899999999999999 47:18.49"));
		assertEquals(map.get("2"),
			VectorUtil.getVector("$48$9:4.0 10:6.0 15:8.6 17:6.0 18:9.0 23:12.899999999999999 41:8.6 42:12.899999999999999 47:18.49"));

	}

	@Test
	public void pipelineStreamTest() throws Exception {
		new VectorInteraction().setSelectedCols(new String[] {"c1", "c2"})
			.setOutputCol("product_result").transform((StreamOperator)getData(false)).print();
		StreamOperator.execute();
	}

}