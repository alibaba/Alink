package com.alibaba.alink.pipeline;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.PipelinePredictBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.dataproc.JsonValue;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.pipeline.feature.MultiHotEncoder;
import com.alibaba.alink.pipeline.feature.OneHotEncoder;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class PipelinePredictBatchOpTest extends AlinkTestBase {

	 @Test
	 public void test() throws Exception {

		Row[] trainArray = new Row[] {
			Row.of("u0", "1.0 1.0", 1.0, 1.0, 1, 18),
			Row.of("u1", "1.0 1.0", 1.0, 1.0, 0, 19),
			Row.of("u2", "1.0 0.0", 1.0, 0.0, 1, 88),
			Row.of("u3", "1.0 0.0", 1.0, 0.0, 1, 18),
			Row.of("u4", "0.0 1.0", 0.0, 1.0, 1, 88),
			Row.of("u5", "0.0 1.0", 0.0, 1.0, 1, 19),
			Row.of("u6", "0.0 1.0", 0.0, 1.0, 1, 88)
		};
		BatchOperator <?> trainData = new MemSourceBatchOp(Arrays.asList(trainArray),
				new String[] {"uid", "uf", "f0", "f1", "labels", "iid"});

		String[] oneHotCols = new String[] {"uid", "f0", "f1", "iid"};
		String[] multiHotCols = new String[] {"uf"};

		Pipeline pipe = new Pipeline()
			.add(
				new OneHotEncoder()
					.setSelectedCols(oneHotCols)
					.setOutputCols("ovec"))
			.add(
				new MultiHotEncoder().setDelimiter(" ")
					.setSelectedCols(multiHotCols)
					.setOutputCols("mvec"))
			.add(
				new VectorAssembler()
					.setSelectedCols("ovec", "mvec")
					.setOutputCol("vec"))
			.add(
				new LogisticRegression()
					.setVectorCol("vec")
					.setLabelCol("labels")
					.setReservedCols("uid", "iid")
					.setPredictionDetailCol("detail")
					.setPredictionCol("pred"))
			.add(
				new JsonValue()
					.setSelectedCol("detail")
					.setJsonPath("$.1")
					.setOutputCols("score"));
		    BatchOperator model = pipe.fit(trainData).save();
			String path = "/tmp/pipeline_predict_batch_op_test.ak";
			model.link(new AkSinkBatchOp().setFilePath(path).setOverwriteSink(true));
			BatchOperator.execute();

			new PipelinePredictBatchOp().setModelFilePath(path).linkFrom(trainData).print();
	}
}