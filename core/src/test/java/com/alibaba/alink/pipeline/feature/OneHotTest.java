package com.alibaba.alink.pipeline.feature;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.OneHotPredictBatchOp;
import com.alibaba.alink.operator.batch.feature.OneHotTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OneHotTest {
	TableSchema schema = new TableSchema(
		new String[] {"id", "docid", "word", "cnt"},
		new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.STRING, Types.LONG}
	);
	String[] binaryNames = new String[] {"docid", "word", "cnt"};

	AlgoOperator getData(boolean isBatch) {

		Row[] array = new Row[] {
			Row.of(new Object[] {"0", "doc0", "天", 4L}),
			Row.of(new Object[] {"1", "doc0", "地", 5L}),
			Row.of(new Object[] {"2", "doc0", "人", 1L}),
			Row.of(new Object[] {"3", "doc1", null, 3L}),
			Row.of(new Object[] {"4", null, "人", 2L}),
			Row.of(new Object[] {"5", "doc1", "合", 4L}),
			Row.of(new Object[] {"6", "doc1", "一", 4L}),
			Row.of(new Object[] {"7", "doc2", "清", 3L}),
			Row.of(new Object[] {"8", "doc2", "一", 2L}),
			Row.of(new Object[] {"9", "doc2", "色", 2L})
		};

		if (isBatch) {
			return new MemSourceBatchOp(Arrays.asList(array), schema);
		} else {
			return new MemSourceStreamOp(Arrays.asList(array), schema);
		}
	}

	@Test
	public void pipelineTest() throws Exception {

		OneHotEncoder oneHot = new OneHotEncoder()
			.setSelectedCols(binaryNames)
			.setDropLast(false)
			.setOutputCol("results")
			.setIgnoreNull(true);

		VectorAssembler va = new VectorAssembler()
			.setSelectedCols(new String[] {"cnt", "results"})
			.setOutputCol("outN");

		Pipeline pl = new Pipeline().add(oneHot).add(va);

		PipelineModel model = pl.fit((BatchOperator)getData(true));
		Row[] parray = new Row[] {
			Row.of(new Object[] {"0", "doc0", "天", 4L}),
			Row.of(new Object[] {"1", "doc2", null, 3L})
		};

		// batch predict
		MemSourceBatchOp predData = new MemSourceBatchOp(Arrays.asList(parray), schema);
		BatchOperator result = model.transform(predData).select(new String[]{"docid", "outN"});

		List<Row> rows = result.collect();

		for (Row row : rows) {
			if (row.getField(0).toString().equals("doc0")) {
				Assert.assertEquals(row.getField(1).toString(),"$17$0:4.0 3:1.0 9:1.0 14:1.0");
			} else if (row.getField(0).toString().equals("doc2")) {
				Assert.assertEquals(row.getField(1).toString(),"$17$0:3.0 1:1.0 13:1.0 16:1.0");
			}
		}

		// stream predict
		MemSourceStreamOp predSData = new MemSourceStreamOp(Arrays.asList(parray), schema);
		model.transform(predSData).print();
		StreamOperator.execute();
	}

	@Test
	public void batchTest() throws Exception {
		OneHotTrainBatchOp op = new OneHotTrainBatchOp()
				.setSelectedCols(binaryNames)
				.setDropLast(false)
				.setIgnoreNull(true);
		OneHotTrainBatchOp model = op.linkFrom((BatchOperator)getData(true));
		OneHotPredictBatchOp predict = new OneHotPredictBatchOp().setOutputCol("results");
		Row[] parray = new Row[] {
				Row.of(new Object[] {"0", "doc0", "天", 4L}),
				Row.of(new Object[] {"1", "doc2", null, 3L})
		};
		// batch predict
		MemSourceBatchOp predData = new MemSourceBatchOp(Arrays.asList(parray), schema);

		BatchOperator res = predict.linkFrom(model, predData);
		List rows = res.getDataSet().collect();
		HashMap<String, Vector> map = new HashMap<String, Vector>();
		map.put((String) ((Row) rows.get(0)).getField(0), VectorUtil.getVector(((Row) rows.get(0)).getField(4)));
		map.put((String) ((Row) rows.get(1)).getField(0), VectorUtil.getVector(((Row) rows.get(1)).getField(4)));
		assertEquals(map.get("0"),
			VectorUtil.getVector("$16$2:1.0 8:1.0 13:1.0"));
		assertEquals(map.get("1"),
			VectorUtil.getVector("$16$0:1.0 12:1.0 15:1.0"));
	}
}