package com.alibaba.alink.operator.stream.clustering;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests the {@link StreamingKMeansStreamOp}.
 */
public class StreamingKMeansStreamOpTest extends AlinkTestBase {
	private BatchOperator<?> trainDataBatchOp;
	private StreamOperator<?> predictDataStreamOp;
	private int numElementsToPredict = 100;

	@Before
	public void before() {
		Row[] trainDataArray = new Row[] {
			Row.of(0, "0 0 0"),
			Row.of(1, "0.1 0.1 0.1"),
			Row.of(2, "0.2 0.2 0.2"),
			Row.of(3, "9 9 9"),
			Row.of(4, "9.1 9.1 9.1"),
			Row.of(5, "9.2 9.2 9.2")
		};
		Row[] predictDataArray = new Row[numElementsToPredict];
		for (int i = 0; i < predictDataArray.length; i++) {
			predictDataArray[i] = Row.of(DenseVector.rand(3));
		}
		trainDataBatchOp = new MemSourceBatchOp(Arrays.asList(trainDataArray), new String[] {"id", "vec"});
		predictDataStreamOp = new MemSourceStreamOp(Arrays.asList(predictDataArray), new String[] {"vec"});
	}

	@Test
	public void testStreamingKmeans() throws Exception {
		BatchOperator<?> model = new KMeansTrainBatchOp()
			.setVectorCol("vec")
			.setK(2)
			.linkFrom(trainDataBatchOp);
		StreamingKMeansStreamOp streamingKMeansStreamOp = new StreamingKMeansStreamOp(model)
			.setPredictionCol("pred")
			.setTimeInterval(1L)
			.setHalfLife(1)
			.setReservedCols("vec")
			.linkFrom(predictDataStreamOp, predictDataStreamOp);
		CollectSinkStreamOp predSinkData = streamingKMeansStreamOp.link(new CollectSinkStreamOp());
		StreamOperator.execute();
		verifyExecutionResult(predSinkData.getAndRemoveValues());
	}

	private void verifyExecutionResult(List <Row> predResult) {
		Assert.assertEquals(numElementsToPredict, predResult.size());
		Long clusterdId = (Long) predResult.get(0).getField(1);
		for (Row predData : predResult) {
			Assert.assertEquals(clusterdId, predData.getField(1));
		}
	}
}