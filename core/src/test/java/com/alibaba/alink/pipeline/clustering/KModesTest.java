package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Random;

public class KModesTest extends AlinkTestBase {
	private MemSourceBatchOp inputTableBatch;
	private MemSourceBatchOp predictTableBatch;
	private MemSourceStreamOp predictTableStream;
	private String[] featureColNames;
	int numDataPoints = 100;
	int numFeatures = 10;

	@Before
	public void setUp() throws Exception {
		Random random = new Random(2018L);
		String[] alphaTable = new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"};
		Object[][] objs = new Object[numDataPoints][numFeatures + 1];
		String[] colNames = new String[numFeatures + 1];
		featureColNames = new String[numFeatures];
		colNames[0] = "id";
		for (int i = 0; i < numFeatures; i++) {
			colNames[i + 1] = "col" + i;
			featureColNames[i] = colNames[i + 1];
		}
		for (int i = 0; i < numDataPoints; i++) {
			objs[i][0] = (long) i;
			for (int j = 0; j < numFeatures; j++) {
				objs[i][j + 1] = alphaTable[random.nextInt(12)];
			}
		}
		inputTableBatch = new MemSourceBatchOp(objs, colNames);
		predictTableBatch = new MemSourceBatchOp(objs, colNames);
		predictTableStream = new MemSourceStreamOp(objs, colNames);
	}

	@Test
	public void testKmodes() throws Exception {
		KModes kModes = new KModes()
			.setFeatureCols(featureColNames)
			.setPredictionCol("pred")
			.setK(3);
		Pipeline pipeline = new Pipeline().add(kModes);
		PipelineModel model = pipeline.fit(inputTableBatch);
		BatchOperator <?> batchPredOp = model.transform(predictTableBatch);
		List <Row> batchResult = batchPredOp.collect();

		model.transform(predictTableStream).print();
		StreamOperator.execute();
	}

	private void verifyPredResult(List <Row> batchResult, List <Row> streamResult) {
		compareResultCollections(batchResult, streamResult, new Comparator <Row>() {
			@Override
			public int compare(Row o1, Row o2) {
				for (int i = 1; i <= numFeatures; i++) {
					int cmp = Character.compare(
						((String) o1.getField(i)).charAt(0),
						((String) o2.getField(i)).charAt(0)
					);
					if (cmp != 0) {
						return cmp;
					}
				}
				return 0;
			}
		});
	}
}