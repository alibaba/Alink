package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SoftmaxTest extends AlinkTestBase {

	String labelColName = "label";
	Row[] vecrows = new Row[] {
		Row.of("0:1.0 2:7.0 15:9.0", "1.0 7.0 9.0", 1.0, 7.0, 9.0, 2),
		Row.of("0:1.0 2:3.0 12:3.0", "1.0 3.0 3.0", 1.0, 3.0, 3.0, 3),
		Row.of("0:1.0 2:2.0 10:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 1),
		Row.of("0:1.0 2:2.0 7:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 1)
	};
	String[] veccolNames = new String[] {"svec", "vec", "f0", "f1", "f2", "label"};

	Softmax softmax;

	Softmax vsoftmax;

	Softmax vssoftmax;

	Softmax svsoftmax;

	@Before
	public void setUp() {
		softmax = new Softmax(new Params())
			.setFeatureCols(new String[] {"f0", "f1", "f2"})
			.setStandardization(true)
			.setWithIntercept(true)
			.setEpsilon(1.0e-20)
			.setLabelCol(labelColName).enableLazyPrintModelInfo()
			.setPredictionCol("predLr")
			.setMaxIter(10);

		vsoftmax = new Softmax()
			.setVectorCol("vec")
			.setStandardization(true)
			.setWithIntercept(true)
			.setEpsilon(1.0e-20)
			.setLabelCol(labelColName)
			.setPredictionCol("vpredLr").enableLazyPrintModelInfo()
			.setMaxIter(10);

		vssoftmax = new Softmax()
			.setVectorCol("svec")
			.setStandardization(true)
			.setWithIntercept(true)
			.setEpsilon(1.0e-20)
			.setLabelCol(labelColName)
			.setPredictionCol("vsspredLr").enableLazyPrintModelInfo()
			.setOptimMethod("newton")
			.setMaxIter(10);

		svsoftmax = new Softmax()
			.setVectorCol("svec")
			.setStandardization(true)
			.setWithIntercept(true)
			.setEpsilon(1.0e-20)
			.setLabelCol(labelColName)
			.setPredictionCol("svpredLr")
			.setPredictionDetailCol("svpredDetail").enableLazyPrintModelInfo()
			.setMaxIter(10);
	}

	@Test
	public void pipelineTest() throws Exception {
		BatchOperator<?> vecdata = new MemSourceBatchOp(Arrays.asList(vecrows), veccolNames);
		StreamOperator<?> svecdata = new MemSourceStreamOp(Arrays.asList(vecrows), veccolNames);
		Pipeline pl = new Pipeline().add(softmax).add(vsoftmax).add(svsoftmax).add(vssoftmax);

		PipelineModel model = pl.fit(vecdata);

		BatchOperator<?> result = model.transform(vecdata).select(
			new String[] {"label", "predLr", "vpredLr", "svpredLr"});

		List <Row> data = result.lazyPrint(100).collect();
		for (Row row : data) {
			for (int i = 1; i < 3; ++i) {
				Assert.assertEquals(row.getField(0), row.getField(i));
			}
		}

		// below is stream test code

		// below is stream test code.
		CollectSinkStreamOp sop = model.transform(svecdata).select(
			new String[] {"label", "predLr", "vpredLr", "svpredLr"}).link(new CollectSinkStreamOp());
		StreamOperator.execute();

		List <Row> rows = sop.getAndRemoveValues();

		for (Row row : rows) {
			for (int i = 1; i < 3; ++i) {
				Assert.assertEquals(row.getField(0), row.getField(i));
			}
		}
	}

	@Test
	public void pipelineTest1() {
		BatchOperator<?> vecmdata = new MemSourceBatchOp(Arrays.asList(vecrows), veccolNames);

		Pipeline pl = new Pipeline().add(softmax).add(vsoftmax).add(svsoftmax).add(vssoftmax);

		PipelineModel modelm = pl.fit(vecmdata);

		List <Row> data = modelm.transform(vecmdata)
			.select(new String[] {"label", "predLr", "vpredLr", "svpredLr"}).collect();
		for (Row row : data) {
			for (int i = 1; i < 3; ++i) {
				Assert.assertEquals(row.getField(0), row.getField(i));
			}
		}
	}
}
