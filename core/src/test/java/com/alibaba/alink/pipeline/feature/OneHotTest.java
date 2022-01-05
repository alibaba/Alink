package com.alibaba.alink.pipeline.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.OneHotPredictBatchOp;
import com.alibaba.alink.operator.batch.feature.OneHotTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.OneHotPredictStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test for OneHot.
 */

public class OneHotTest extends AlinkTestBase {
	private final TableSchema schema = new TableSchema(
		new String[] {"id", "docid", "word", "cnt"},
		new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.STRING, Types.LONG}
	);
	private final String[] binaryNames = new String[] {"docid", "word", "cnt"};

	AlgoOperator <?> getData(boolean isBatch) {

		Row[] array = new Row[] {
			Row.of("0", "doc0", "天", 4L),
			Row.of("1", "doc0", "地", 5L),
			Row.of("2", "doc0", "人", 1L),
			Row.of("3", "doc1", null, 3L),
			Row.of("4", null, "人", 2L),
			Row.of("5", "doc1", "合", 4L),
			Row.of("6", "doc1", "一", 4L),
			Row.of("7", "doc2", "清", 3L),
			Row.of("8", "doc2", "一", 2L),
			Row.of("9", "doc2", "色", 2L)
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
			.setOutputCols("results")
			.setDropLast(false)
			.enableLazyPrintModelInfo();

		VectorAssembler va = new VectorAssembler()
			.setSelectedCols(new String[] {"cnt", "results"})
			.enableLazyPrintTransformStat("xxxxxx")
			.setOutputCol("outN");

		Pipeline pl = new Pipeline().add(oneHot).add(va);

		PipelineModel model = pl.fit((BatchOperator <?>) getData(true));
		Row[] parray = new Row[] {
			Row.of("0", "doc0", "天", 4L),
			Row.of("1", "doc2", null, 3L)
		};

		List <Row> expectedRow = Arrays.asList(
			Row.of("0", new SparseVector(19, new int[] {0, 3, 10, 16}, new double[] {4.0, 1.0, 1.0, 1.0})),
			Row.of("1", new SparseVector(19, new int[] {0, 1, 12, 15}, new double[] {3.0, 1.0, 1.0, 1.0}))
		);

		// batch predict
		MemSourceBatchOp predData = new MemSourceBatchOp(Arrays.asList(parray), schema);

		List <Row> rows = model
			.transform(predData)
			.select("id, outN")
			.collect();

		assertListRowEqual(expectedRow, rows, 0);

		// stream predict
		MemSourceStreamOp predSData = new MemSourceStreamOp(Arrays.asList(parray), schema);
		CollectSinkStreamOp sink = model
			.transform(predSData)
			.select("id, outN")
			.link(new CollectSinkStreamOp());
		StreamOperator.execute();

		assertListRowEqual(expectedRow, sink.getAndRemoveValues(), 0);

	}

	@Test
	public void batchTest() throws Exception {
		OneHotTrainBatchOp op = new OneHotTrainBatchOp()
			.setSelectedCols(binaryNames);
		OneHotTrainBatchOp model = op.linkFrom((BatchOperator <?>) getData(true));
		OneHotPredictBatchOp predict = new OneHotPredictBatchOp().setOutputCols("results").setDropLast(false);
		Row[] parray = new Row[] {
			Row.of("0", "doc0", "天", 4L),
			Row.of("1", "doc2", null, 3L)
		};
		// batch predict
		MemSourceBatchOp predData = new MemSourceBatchOp(Arrays.asList(parray), schema);

		BatchOperator <?> res = predict.linkFrom(model, predData);

		List <Row> rows = res.getDataSet().collect();
		HashMap <String, Vector> map = new HashMap <String, Vector>();
		map.put((String) ((Row) rows.get(0)).getField(0), VectorUtil.getVector(((Row) rows.get(0)).getField(4)));
		map.put((String) ((Row) rows.get(1)).getField(0), VectorUtil.getVector(((Row) rows.get(1)).getField(4)));
		assertEquals(map.get("0").size(),
			VectorUtil.getVector("$18$2:1.0 9:1.0 14:1.0").size());
		assertEquals(map.get("1").size(),
			VectorUtil.getVector("$18$1:1.0 11:1.0 15:1.0").size());
	}

	@Test
	public void testInitializer() {
		OneHotEncoderModel model = new OneHotEncoderModel();
		Assert.assertEquals(model.getParams().size(), 0);

		OneHotEncoder op = new OneHotEncoder(new Params());
		Assert.assertEquals(op.getParams().size(), 0);

		BatchOperator <?> b = new OneHotTrainBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new OneHotTrainBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		b = new OneHotPredictBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new OneHotPredictBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator <?> s = new OneHotPredictStreamOp(b);
		Assert.assertEquals(s.getParams().size(), 0);
		s = new OneHotPredictStreamOp(b, new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}

}