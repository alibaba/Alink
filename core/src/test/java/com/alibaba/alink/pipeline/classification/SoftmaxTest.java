package com.alibaba.alink.pipeline.classification;

import java.util.Arrays;
import java.util.List;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;

import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

public class SoftmaxTest {

	String labelColName = "label";
	Row[] vecrows = new Row[] {
		Row.of("0:1.0 2:7.0 4:9.0", "1.0 7.0 9.0", 1.0, 7.0, 9.0, 2),
		Row.of("0:1.0 2:3.0 4:3.0", "1.0 3.0 3.0", 1.0, 3.0, 3.0, 3),
		Row.of("0:1.0 2:2.0 4:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 1),
		Row.of("0:1.0 2:2.0 4:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 1)
	};
	String[] veccolNames = new String[] {"svec", "vec", "f0", "f1", "f2", "label"};
	BatchOperator vecdata = new MemSourceBatchOp(Arrays.asList(vecrows), veccolNames);
	StreamOperator svecdata = new MemSourceStreamOp(Arrays.asList(vecrows), veccolNames);

	@Test
	public void pipelineTest() throws Exception {
		MLEnvironmentFactory.getDefault().getExecutionEnvironment().setParallelism(4);
		Softmax softmax = new Softmax()
			.setFeatureCols(new String[] {"f0", "f1", "f2"})
			.setStandardization(true)
			.setWithIntercept(true)
			.setEpsilon(1.0e-20)
			.setLabelCol(labelColName)
			.setPredictionCol("predLr")
			.setMaxIter(10000);

		Softmax vsoftmax = new Softmax()
			.setVectorCol("vec")
			.setStandardization(true)
			.setWithIntercept(true)
			.setEpsilon(1.0e-20)
			.setLabelCol(labelColName)
			.setPredictionCol("vpredLr")
			.setMaxIter(10000);

		Softmax svsoftmax = new Softmax()
			.setVectorCol("svec")
			.setStandardization(true)
			.setWithIntercept(true)
			.setEpsilon(1.0e-20)
			.setLabelCol(labelColName)
			.setPredictionCol("svpredLr")
			.setPredictionDetailCol("svpredDetail")
			.setMaxIter(10000);

		Pipeline pl = new Pipeline().add(softmax).add(vsoftmax).add(svsoftmax);

		PipelineModel model = pl.fit(vecdata);

		BatchOperator result = model.transform(vecdata).select(
			new String[] {"label", "predLr", "vpredLr", "svpredLr"});

		List<Row> data = result.collect();

		for (Row row : data) {
			for (int i = 1; i < 3; ++i) {
				Assert.assertEquals(row.getField(0), row.getField(i));
			}
		}

		// below is stream test code
		model.transform(svecdata).print();
		StreamOperator.execute();
	}
}
